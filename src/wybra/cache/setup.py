from __future__ import annotations

from asyncio import CancelledError

from wybra.cache.capabilities import CacheCapability
from wybra.cache.redis_connection import resolve_redis_urls
from wybra.cache.registry import CachesCapability, build_caches
from wybra.cache.settings import CachesSettings
from wybra.services.secrets import SecretsCapability
from wybra.site import Site, SiteCapabilityProxy


async def setup_site(site: Site) -> None:
    settings = CachesSettings.load_settings(site.config)
    secrets = site.capability_proxy(SecretsCapability)

    async def finalise_cache_setup() -> None:
        await _finalise_cache_setup(site, settings, secrets)

    site.defer_setup_finalisation(finalise_cache_setup)


async def _finalise_cache_setup(
    site: Site,
    settings: CachesSettings,
    secrets: SiteCapabilityProxy[SecretsCapability],
) -> None:
    secret_capability = None
    if any(instance.requires_secret_resolution for instance in settings.instances):
        secret_capability = await secrets.finalise_optional()
    redis_urls = resolve_redis_urls(settings, secret_capability)
    caches = await build_caches(settings, redis_urls=redis_urls)
    try:
        site.provide_capability(CacheCapability, caches.require("default").values)
        site.provide_capability(CachesCapability, caches)
    except BaseException as setup_error:
        try:
            await caches.close()
        except CancelledError as close_error:
            close_error.add_note(
                f"Cache capability registration also failed with "
                f"{type(setup_error).__name__}: {setup_error}"
            )
            raise close_error from setup_error
        except BaseException as close_error:
            if isinstance(setup_error, CancelledError):
                setup_error.add_note(
                    f"Cache capability cleanup also failed with "
                    f"{type(close_error).__name__}: {close_error}"
                )
                raise setup_error from close_error
            raise BaseExceptionGroup(
                "Cache capability registration and cleanup failed.",
                [setup_error, close_error],
            ) from setup_error
        raise


__all__ = ("setup_site",)
