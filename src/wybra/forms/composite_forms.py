"""Fixed-member, transactional Tortoise composite forms."""

from __future__ import annotations

from collections.abc import Mapping
from copy import copy
from dataclasses import dataclass

from tortoise.backends.base.client import BaseDBAsyncClient
from tortoise.fields.relational import ManyToManyFieldInstance
from tortoise.models import Model

from wybra.db.capabilities import tortoise_transaction_for_route
from wybra.db.routing import DbConnection
from wybra.db.versioning import (
    OptimisticLockConflict,
    VersionField,
    save_model_update,
    version_field_name,
)
from wybra.events import observe
from wybra.events.forms import persistence_event
from wybra.forms.fields import Field, Form, FormError, SaveResult, UnknownFieldPolicy
from wybra.forms.model_forms import (
    ModelBindingError,
    ModelForm,
    ModelFormDeclarationError,
)


@dataclass(frozen=True, slots=True)
class ModelOf:
    """A deferred fixed related-model member of a composite form."""

    owner: type[Model]
    relation_name: str


def model_of(owner: type[Model], relation_name: str) -> ModelOf:
    """Declare a fixed related member resolved after Tortoise finalisation."""
    return ModelOf(owner=owner, relation_name=relation_name)


@dataclass(frozen=True, slots=True)
class _Member:
    name: str | None
    model: type[Model]
    owner: type[Model] | None = None
    relation_name: str | None = None


class CompositeForm(Form):
    """One form that atomically persists a fixed group of related models."""

    def __init__(
        self,
        *,
        connection: DbConnection,
        instances: Mapping[str | None, Model] | None = None,
        defaults: Mapping[str, object] | None = None,
        values: Mapping[str, object] | None = None,
        options: Mapping[str, Mapping[str, str]] | None = None,
        unknown_fields: UnknownFieldPolicy = "ignore",
    ) -> None:
        self.connection = connection
        self._writer_route = connection.for_write()
        self.members = self._declared_members()
        self.instances = dict(instances or {})
        self._validate_instances()
        super().__init__(
            defaults=defaults,
            values=values,
            options=options,
            unknown_fields=unknown_fields,
        )

    @classmethod
    def declared_fields(cls) -> dict[str, Field]:
        declared = super().declared_fields()
        fields: dict[str, Field] = {}
        for member in cls._declared_members():
            for name, model_field in member.model._meta.fields_map.items():
                if model_field.pk or getattr(model_field, "generated", False):
                    continue
                if getattr(model_field, "related_model", None) is not None:
                    continue
                if (
                    name.endswith("_id")
                    and name.removesuffix("_id") in member.model._meta.fetch_fields
                ):
                    continue
                qualified = name if member.name is None else f"{member.name}__{name}"
                fields[qualified] = declared.get(
                    qualified,
                    ModelForm.form_field_from_model(model_field),
                )
        unknown = set(declared) - set(fields)
        if unknown:
            raise ModelFormDeclarationError(
                "Unknown composite form field(s): " + ", ".join(sorted(unknown))
            )
        allowed = getattr(getattr(cls, "Meta", None), "fields", None)
        if allowed is None:
            return fields
        unknown_allowed = set(allowed) - set(fields)
        if unknown_allowed:
            raise ModelFormDeclarationError(
                "Unknown composite model field(s): "
                + ", ".join(sorted(unknown_allowed))
            )
        return {name: fields[name] for name in allowed}

    @observe(persistence_event)
    async def save(self) -> SaveResult:
        self._stale_conflict = False
        return await self._save_members()

    async def _save_members(self) -> SaveResult:
        if not self.result.is_valid:
            raise FormError("Cannot save an invalid form.")
        client_scope = tortoise_transaction_for_route(
            self.connection, self._writer_route
        )
        saved: dict[str | None, Model] = {}
        results: list[SaveResult] = []
        try:
            async with client_scope as client:
                for member in self.members:
                    instance = await self._member_instance(member, client)
                    original = copy(instance) if instance._saved_in_db else None
                    self._apply_member_fields(member, instance)
                    if member.name is None:
                        for related_member in self.members:
                            if (
                                related_member.owner is member.model
                                and related_member.relation_name is not None
                            ):
                                setattr(
                                    instance,
                                    related_member.relation_name,
                                    saved[related_member.name],
                                )
                    changed_fields = self._member_changed_fields(
                        member, original, instance
                    )
                    if original is None or changed_fields:
                        await save_model_update(
                            instance,
                            client=client,
                            expected_version=self._submitted_version(member),
                        )
                    saved[member.name] = instance
                    results.append(
                        SaveResult(
                            primary=instance,
                            original=original,
                            changed_fields=changed_fields,
                            created=original is None,
                            updated=original is not None and bool(changed_fields),
                            affected_count=(
                                1 if original is None or changed_fields else 0
                            ),
                        )
                    )
        except OptimisticLockConflict:
            self._stale_conflict = True
            self.add_error(None, "This record was changed by another user.")
            primary = saved.get(None, self.instances.get(None))
            if primary is None:
                raise ModelBindingError(
                    "A composite version conflict requires an existing primary "
                    "instance."
                ) from None
            return SaveResult(primary=primary, original=self.instances.get(None))

        primary = saved[None]
        return SaveResult(
            primary=primary,
            original=self.instances.get(None),
            changed_fields=tuple(
                dict.fromkeys(
                    field_name
                    for result in results
                    for field_name in result.changed_fields
                )
            ),
            created=any(result.created for result in results),
            updated=any(result.updated for result in results),
            affected_count=sum(result.affected_count for result in results),
            member_results=tuple(results),
        )

    @classmethod
    def _declared_members(cls) -> tuple[_Member, ...]:
        declared = getattr(getattr(cls, "Meta", None), "models", None)
        if not isinstance(declared, tuple) or not declared:
            raise ModelFormDeclarationError(
                "CompositeForm declarations require a non-empty Meta.models tuple."
            )
        primary = declared[-1]
        if not isinstance(primary, type) or not issubclass(primary, Model):
            raise ModelFormDeclarationError(
                "The final CompositeForm Meta.models member must be a Tortoise model."
            )
        members: list[_Member] = []
        for declaration in declared[:-1]:
            members.append(cls._resolve_member(declaration, primary))
        members.append(_Member(name=None, model=primary))
        return tuple(members)

    @staticmethod
    def _resolve_member(declaration: object, primary: type[Model]) -> _Member:
        if isinstance(declaration, ModelOf):
            if declaration.owner is not primary:
                raise ModelFormDeclarationError(
                    "model_of owner must be the CompositeForm primary model."
                )
            field = primary._meta.fields_map.get(declaration.relation_name)
            related = getattr(field, "related_model", None)
            if isinstance(field, ManyToManyFieldInstance) or not (
                isinstance(related, type) and issubclass(related, Model)
            ):
                raise ModelFormDeclarationError(
                    "model_of requires a fixed forward Tortoise relation field: "
                    + declaration.relation_name
                )
            return _Member(
                name=declaration.relation_name,
                model=related,
                owner=primary,
                relation_name=declaration.relation_name,
            )
        if not isinstance(declaration, type) or not issubclass(declaration, Model):
            raise ModelFormDeclarationError(
                "CompositeForm Meta.models members must be Tortoise models or "
                "model_of()."
            )
        candidates = [
            (name, field)
            for name, field in primary._meta.fields_map.items()
            if getattr(field, "related_model", None) is declaration
            and not isinstance(field, ManyToManyFieldInstance)
        ]
        if len(candidates) != 1:
            raise ModelFormDeclarationError(
                f"CompositeForm cannot infer one fixed {declaration.__name__} relation "
                f"from {primary.__name__}."
            )
        name, _field = candidates[0]
        return _Member(name=name, model=declaration, owner=primary, relation_name=name)

    def _apply_member_fields(self, member: _Member, instance: Model) -> None:
        for qualified_name, result in self.result.fields.items():
            prefix, field_name = _field_target(qualified_name)
            if (
                prefix != member.name
                or not result.is_valid
                or self.fields[qualified_name].disabled
                or isinstance(instance._meta.fields_map[field_name], VersionField)
            ):
                continue
            setattr(instance, field_name, result.value)

    def _member_changed_fields(
        self,
        member: _Member,
        original: Model | None,
        instance: Model,
    ) -> tuple[str, ...]:
        changed: list[str] = []
        for name, result in self.result.fields.items():
            prefix, field_name = _field_target(name)
            if (
                prefix != member.name
                or not result.is_valid
                or self.fields[name].disabled
                or isinstance(instance._meta.fields_map[field_name], VersionField)
            ):
                continue
            if original is None or getattr(original, field_name) != getattr(
                instance, field_name
            ):
                changed.append(name)
        return tuple(changed)

    async def _member_instance(
        self, member: _Member, client: BaseDBAsyncClient
    ) -> Model:
        supplied = self.instances.get(member.name)
        if supplied is not None:
            return supplied
        primary = self.instances.get(None)
        if primary is not None and member.owner is type(primary):
            related_id = getattr(primary, f"{member.relation_name}_id", None)
            if related_id is not None:
                return await member.model.filter(pk=related_id).using_db(client).get()
        return member.model()

    def _submitted_version(self, member: _Member) -> int | None:
        name = version_field_name(member.model)
        if name is None:
            return None
        qualified = name if member.name is None else f"{member.name}__{name}"
        result = self.result.fields.get(qualified)
        if result is None or not result.is_valid or not isinstance(result.value, int):
            raise ModelBindingError(
                "Versioned CompositeForm members require a valid submitted "
                "version field."
            )
        return result.value

    def _validate_instances(self) -> None:
        valid = {member.name: member.model for member in self.members}
        unknown = set(self.instances) - set(valid)
        if unknown:
            raise ModelBindingError(
                "Unknown composite instance member(s): "
                + ", ".join(sorted(str(name) for name in unknown))
            )
        for name, instance in self.instances.items():
            if not isinstance(instance, valid[name]):
                raise ModelBindingError(
                    f"Invalid composite instance for member: {name}."
                )


def _field_target(name: str) -> tuple[str | None, str]:
    if "__" not in name:
        return None, name
    prefix, field_name = name.split("__", 1)
    return prefix, field_name


__all__ = ("CompositeForm", "ModelOf", "model_of")
