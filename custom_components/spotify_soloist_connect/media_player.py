"""Media player platform for Spotify Soloist Connect."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import websockets
from websockets import ClientConnection

from homeassistant.components.media_player import (
    MediaPlayerDeviceClass,
    MediaPlayerEntity,
    MediaPlayerEntityFeature,
    MediaPlayerState,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_HOST, CONF_PORT
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.typing import ConfigType, DiscoveryInfoType

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

RECONNECT_DELAY = 5


async def async_setup_platform(
    hass: HomeAssistant,
    config: ConfigType,
    async_add_entities: AddEntitiesCallback,
    discovery_info: DiscoveryInfoType | None = None,
) -> None:
    """Set up the media player platform."""
    return


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Spotify Soloist from a config entry."""

    host = entry.data[CONF_HOST]
    port = entry.data[CONF_PORT]

    player = SpotifySoloistPlayer(hass, host, port)

    async_add_entities([player])

    await player.async_start()


class SpotifySoloistPlayer(MediaPlayerEntity):
    """Representation of a Spotify Soloist player."""

    _attr_device_class = MediaPlayerDeviceClass.SPEAKER
    _attr_name = "Spotify Soloist"
    _attr_has_entity_name = True

    _attr_supported_features = (
        MediaPlayerEntityFeature.PLAY
        | MediaPlayerEntityFeature.PAUSE
        | MediaPlayerEntityFeature.NEXT_TRACK
        | MediaPlayerEntityFeature.PREVIOUS_TRACK
        | MediaPlayerEntityFeature.SEEK
        | MediaPlayerEntityFeature.VOLUME_SET
        | MediaPlayerEntityFeature.VOLUME_MUTE
        | MediaPlayerEntityFeature.SHUFFLE_SET
        | MediaPlayerEntityFeature.REPEAT_SET
        | MediaPlayerEntityFeature.PLAY_MEDIA
    )

    def __init__(
        self,
        hass: HomeAssistant,
        host: str,
        port: int,
    ) -> None:
        """Initialize the player."""

        self.hass = hass
        self._host = host
        self._port = port

        self._ws: ClientConnection | None = None
        self._task: asyncio.Task | None = None

        self._available = False
        self._logged_in = False
        self._is_active = False

        self._state: MediaPlayerState | None = None

        self._volume = 0.0
        self._muted = False
        self._shuffle = False
        self._repeat = "off"

        self._media_title: str | None = None
        self._media_artist: str | None = None
        self._media_album: str | None = None
        self._media_image_url: str | None = None
        self._media_content_id: str | None = None
        self._media_duration: float | None = None
        self._media_position = 0.0

        self._position_timestamp = 0.0
        self._position_speed = 0.0

        self._attr_unique_id = f"spotify_soloist_{host}_{port}"

    @property
    def available(self) -> bool:
        """Return whether the player is available."""
        return self._available

    @property
    def state(self) -> MediaPlayerState | None:
        """Return the playback state."""
        return self._state

    @property
    def volume_level(self) -> float:
        """Return the volume level."""
        return self._volume

    @property
    def is_volume_muted(self) -> bool:
        """Return whether volume is muted."""
        return self._muted

    @property
    def shuffle(self) -> bool:
        """Return whether shuffle is enabled."""
        return self._shuffle

    @property
    def repeat(self) -> str:
        """Return the repeat mode."""
        return self._repeat

    @property
    def media_title(self) -> str | None:
        """Return the media title."""
        return self._media_title

    @property
    def media_artist(self) -> str | None:
        """Return the media artist."""
        return self._media_artist

    @property
    def media_album_name(self) -> str | None:
        """Return the media album."""
        return self._media_album

    @property
    def media_image_url(self) -> str | None:
        """Return the media image URL."""
        return self._media_image_url

    @property
    def media_content_id(self) -> str | None:
        """Return the media content ID."""
        return self._media_content_id

    @property
    def media_duration(self) -> float | None:
        """Return the media duration."""
        return self._media_duration

    @property
    def media_position(self) -> float:
        """Return the current media position."""
        if self._position_speed <= 0:
            return self._media_position

        elapsed = time.time() - self._position_timestamp
        return self._media_position + elapsed * self._position_speed

    async def async_start(self) -> None:
        """Start the WebSocket listener."""
        self._task = self.hass.async_create_background_task(
            self._websocket_loop(),
            "spotify_soloist_websocket",
        )

    async def _websocket_loop(self) -> None:
        """Maintain the WebSocket connection."""

        uri = f"ws://{self._host}:{self._port}"

        while True:
            try:
                _LOGGER.debug("Connecting to Spotify Soloist at %s", uri)

                async with websockets.connect(
                    uri,
                    ping_interval=20,
                    ping_timeout=20,
                ) as websocket:
                    self._ws = websocket
                    self._available = True
                    self.async_write_ha_state()

                    await self._send_command("get_auth_state")

                    async for message in websocket:
                        await self._handle_message(message)

            except asyncio.CancelledError:
                raise

            except Exception as err:
                _LOGGER.warning(
                    "Spotify Soloist connection failed: %s",
                    err,
                )

            finally:
                self._ws = None
                self._available = False
                self.async_write_ha_state()

            await asyncio.sleep(RECONNECT_DELAY)

    async def _handle_message(self, message: str) -> None:
        """Handle a WebSocket message."""

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            _LOGGER.warning("Invalid JSON from Spotify Soloist")
            return

        event_type = data.get("type")

        if event_type == "auth_state":
            self._handle_auth_state(data)

        elif event_type == "playback_state":
            self._handle_playback_state(data)

        elif event_type == "playback_changed":
            self._handle_playback_changed(data)

        elif event_type == "track_changed":
            self._handle_track_changed(data)

        elif event_type == "volume_changed":
            self._volume = data.get("volume", 0) / 100
            self.async_write_ha_state()

        elif event_type == "options_changed":
            self._handle_options_changed(data)

        elif event_type == "position_sync":
            self._handle_position_sync(data)

        elif event_type == "device_changed":
            self._is_active = data.get("is_active", False)
            self.async_write_ha_state()

        elif event_type == "error":
            _LOGGER.error(
                "Spotify Soloist error: %s",
                data.get("message"),
            )

    def _handle_auth_state(self, data: dict[str, Any]) -> None:
        """Handle authentication state."""

        self._logged_in = data.get("logged_in", False)
        self._is_active = data.get("is_active", False)

        if not self._logged_in:
            self._state = MediaPlayerState.IDLE

        self.async_write_ha_state()

    def _handle_playback_state(self, data: dict[str, Any]) -> None:
        """Handle a complete playback state."""

        status = data.get("status")

        if status == "playing":
            self._state = MediaPlayerState.PLAYING
        elif status == "paused":
            self._state = MediaPlayerState.PAUSED
        elif status == "buffering":
            self._state = MediaPlayerState.BUFFERING
        else:
            self._state = MediaPlayerState.IDLE

        self._volume = data.get("volume", 0) / 100

        self._is_active = data.get("is_active", False)

        options = data.get("options", {})

        self._shuffle = options.get("shuffle", False)
        self._repeat = options.get("repeat", "off")

        position = data.get("position", {})

        self._media_position = position.get(
            "position_ms",
            0,
        ) / 1000

        self._position_timestamp = time.time()
        self._position_speed = position.get("speed", 0)

        item = data.get("item")

        if item:
            self._update_media(item)

        self.async_write_ha_state()

    def _handle_playback_changed(self, data: dict[str, Any]) -> None:
        """Handle playback state changes."""

        status = data.get("status")

        if status == "playing":
            self._state = MediaPlayerState.PLAYING
            self._position_timestamp = time.time()
            self._position_speed = 1

        elif status == "paused":
            self._update_position()
            self._state = MediaPlayerState.PAUSED
            self._position_speed = 0

        elif status == "buffering":
            self._state = MediaPlayerState.BUFFERING

        else:
            self._state = MediaPlayerState.IDLE
            self._position_speed = 0

        self.async_write_ha_state()

    def _handle_track_changed(self, data: dict[str, Any]) -> None:
        """Handle track changes."""

        item = data.get("item")

        if item:
            self._update_media(item)

        self.async_write_ha_state()

    def _handle_options_changed(self, data: dict[str, Any]) -> None:
        """Handle playback option changes."""

        options = data.get("options", {})

        self._shuffle = options.get(
            "shuffle",
            self._shuffle,
        )

        self._repeat = options.get(
            "repeat",
            self._repeat,
        )

        self.async_write_ha_state()

    def _handle_position_sync(self, data: dict[str, Any]) -> None:
        """Handle position synchronization."""

        position = data.get("position", {})

        self._media_position = position.get(
            "position_ms",
            0,
        ) / 1000

        self._position_timestamp = time.time()
        self._position_speed = position.get("speed", 0)

        self.async_write_ha_state()

    def _update_media(self, item: dict[str, Any]) -> None:
        """Update current media information."""

        self._media_content_id = item.get("uri")

        decorations = item.get("decorations", {})
        identity = decorations.get("identity", {})

        self._media_title = identity.get("name")

        creators = decorations.get("creators", [])

        if creators:
            creator = creators[0].get("entity", {})
            creator_identity = creator.get(
                "decorations",
                {},
            ).get("identity", {})

            self._media_artist = creator_identity.get("name")

        parent = decorations.get("parent", {})
        parent_entity = parent.get("entity", {})

        parent_identity = parent_entity.get(
            "decorations",
            {},
        ).get("identity", {})

        self._media_album = parent_identity.get("name")

        covers = decorations.get(
            "visual_identity",
            {},
        ).get("cover", [])

        if covers:
            self._media_image_url = covers[-1].get("url")

        playback = decorations.get("playback", {})

        duration_ms = playback.get("duration_ms")

        if duration_ms is not None:
            self._media_duration = duration_ms / 1000

    def _update_position(self) -> None:
        """Update the interpolated position."""

        if self._position_speed <= 0:
            return

        elapsed = time.time() - self._position_timestamp

        self._media_position += (
            elapsed * self._position_speed
        )

        self._position_timestamp = time.time()

    async def _send_command(
        self,
        command: str,
        **fields: Any,
    ) -> None:
        """Send a command to Spotify Soloist."""

        if self._ws is None:
            return

        message = {
            "type": "command",
            "command": command,
            **fields,
        }

        await self._ws.send(json.dumps(message))

    async def async_media_play(self) -> None:
        """Play."""
        await self._send_command("play")

    async def async_media_pause(self) -> None:
        """Pause."""
        await self._send_command("pause")

    async def async_media_next_track(self) -> None:
        """Skip to the next track."""
        await self._send_command("skip_next")

    async def async_media_previous_track(self) -> None:
        """Skip to the previous track."""
        await self._send_command("skip_prev")

    async def async_set_volume_level(
        self,
        volume: float,
    ) -> None:
        """Set volume."""
        await self._send_command(
            "set_volume",
            volume=round(volume * 100),
        )

    async def async_mute_volume(
        self,
        mute: bool,
    ) -> None:
        """Mute volume."""

        self._muted = mute

        if mute:
            await self._send_command(
                "set_volume",
                volume=0,
            )
        else:
            await self._send_command(
                "set_volume",
                volume=round(self._volume * 100),
            )

    async def async_media_seek(
        self,
        position: float,
    ) -> None:
        """Seek to a position."""
        await self._send_command(
            "seek",
            position_ms=round(position * 1000),
        )

    async def async_set_shuffle(
        self,
        shuffle: bool,
    ) -> None:
        """Set shuffle."""
        await self._send_command(
            "set_shuffle",
            enabled=shuffle,
        )

    async def async_set_repeat(
        self,
        repeat: str,
    ) -> None:
        """Set repeat mode."""

        if repeat == "off":
            await self._send_command(
                "set_repeat_track",
                enabled=False,
            )
            await self._send_command(
                "set_repeat_context",
                enabled=False,
            )

        elif repeat == "all":
            await self._send_command(
                "set_repeat_track",
                enabled=False,
            )
            await self._send_command(
                "set_repeat_context",
                enabled=True,
            )

        elif repeat == "one":
            await self._send_command(
                "set_repeat_context",
                enabled=False,
            )
            await self._send_command(
                "set_repeat_track",
                enabled=True,
            )

    async def async_play_media(
        self,
        media_type: str,
        media_id: str,
        **kwargs: Any,
    ) -> None:
        """Play a Spotify URI."""

        await self._send_command(
            "play",
            uri=media_id,
        )
