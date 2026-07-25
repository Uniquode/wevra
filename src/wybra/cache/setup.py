from __future__ import annotations

from asyncio import CancelledError

from wybra.cache.capabilities import CacheCapability
from wybra.cache.registry import CachesCapability, build_caches
from wybra.cache.settings import CachesSettings
from wybra.site import Site


async def setup_site(site: Site) -> None:
    settings = CachesSettings.load_settings(site.config)
    caches = await build_caches(settings)
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
