"""Config flow for Spotify Soloist Connect."""

from __future__ import annotations

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from .const import DEFAULT_PORT, DOMAIN


async def validate_connection(
    hass: HomeAssistant,
    host: str,
    port: int,
) -> None:
    """Validate the connection to Spotify Soloist."""
    try:
        import websockets

        uri = f"ws://{host}:{port}"

        async with websockets.connect(uri, open_timeout=5):
            return

    except Exception as err:
        raise CannotConnect from err


class SpotifySoloistConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a Spotify Soloist Connect config flow."""

    VERSION = 1

    async def async_step_user(
        self,
        user_input: dict | None = None,
    ) -> config_entries.FlowResult:
        """Handle the initial setup step."""
        errors: dict[str, str] = {}

        if user_input is not None:
            host = user_input[CONF_HOST]
            port = user_input[CONF_PORT]

            try:
                await validate_connection(
                    self.hass,
                    host,
                    port,
                )
            except CannotConnect:
                errors["base"] = "cannot_connect"
            except Exception:
                errors["base"] = "unknown"
            else:
                await self.async_set_unique_id(
                    f"{host}:{port}"
                )
                self._abort_if_unique_id_configured()

                return self.async_create_entry(
                    title="Spotify Soloist",
                    data={
                        CONF_HOST: host,
                        CONF_PORT: port,
                    },
                )

        schema = vol.Schema(
            {
                vol.Required(
                    CONF_HOST,
                    default="192.168.178.55",
                ): str,
                vol.Required(
                    CONF_PORT,
                    default=DEFAULT_PORT,
                ): vol.Coerce(int),
            }
        )

        return self.async_show_form(
            step_id="user",
            data_schema=schema,
            errors=errors,
        )


class CannotConnect(HomeAssistantError):
    """Error to indicate that the connection failed."""
