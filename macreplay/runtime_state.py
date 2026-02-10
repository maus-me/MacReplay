from dataclasses import dataclass
from typing import Callable, Any


@dataclass
class SchedulerState:
    logger: Any
    job_manager: Any
    get_epg_refresh_interval: Callable[[], float]
    get_channel_refresh_interval: Callable[[], float]


@dataclass
class SettingsState:
    create_settings_blueprint: Callable[..., Any]
    enqueue_epg_refresh: Callable[..., Any]


@dataclass
class EpgState:
    create_epg_blueprint: Callable[..., Any]
    refresh_xmltv: Callable[..., Any]
    refresh_xmltv_for_epg_ids: Callable[..., Any]
    enqueue_epg_refresh: Callable[..., Any]
    get_cached_xmltv: Callable[[], Any]
    get_last_updated: Callable[[], Any]
    get_epg_refresh_status: Callable[[], Any]
    logger: Any
    getPortals: Callable[[], Any]
    get_db_connection: Callable[[], Any]
    effective_epg_name: Callable[..., Any]
    getSettings: Callable[[], Any]
    open_epg_source_db: Callable[..., Any]


@dataclass
class PortalState:
    create_portal_blueprint: Callable[..., Any]
    logger: Any
    getPortals: Callable[[], Any]
    savePortals: Callable[..., Any]
    getSettings: Callable[[], Any]
    get_db_connection: Callable[[], Any]
    ACTIVE_GROUP_CONDITION: str
    channelsdvr_match_status: Any
    channelsdvr_match_status_lock: Any
    normalize_mac_data: Callable[..., Any]
    job_manager: Any
    defaultPortal: Any
    DB_PATH: str
    set_cached_xmltv: Callable[..., Any]
    filter_cache: Any


@dataclass
class EditorState:
    create_editor_blueprint: Callable[..., Any]
    logger: Any
    get_db_connection: Callable[[], Any]
    ACTIVE_GROUP_CONDITION: str
    get_cached_xmltv: Callable[[], Any]
    get_epg_channel_ids: Callable[[], Any]
    get_epg_channel_map: Callable[[], Any]
    getSettings: Callable[[], Any]
    getPortals: Callable[[], Any]
    suggest_channelsdvr_matches: Callable[..., Any]
    host: str
    refresh_epg_for_ids: Callable[..., Any]
    refresh_lineup: Callable[..., Any]
    enqueue_refresh_all: Callable[..., Any]
    set_last_playlist_host: Callable[..., Any]
    filter_cache: Any
    effective_epg_name: Callable[..., Any]


@dataclass
class MiscState:
    create_misc_blueprint: Callable[..., Any]
    LOG_DIR: str
    occupied: Any
    refresh_custom_sources: Callable[..., Any]
    get_epg_source_status: Callable[..., Any]


@dataclass
class HdhrState:
    create_hdhr_blueprint: Callable[..., Any]
    host: str
    getSettings: Callable[[], Any]
    refresh_lineup: Callable[..., Any]
    get_cached_lineup: Callable[[], Any]


@dataclass
class PlaylistState:
    create_playlist_blueprint: Callable[..., Any]
    logger: Any
    host: str
    getPortals: Callable[[], Any]
    getSettings: Callable[[], Any]
    get_db_connection: Callable[[], Any]
    ACTIVE_GROUP_CONDITION: str
    effective_display_name: Callable[..., Any]
    effective_epg_name: Callable[..., Any]
    get_cached_playlist: Callable[[], Any]
    set_cached_playlist: Callable[..., Any]
    get_last_playlist_host: Callable[[], Any]
    set_last_playlist_host: Callable[..., Any]


@dataclass
class StreamingState:
    create_streaming_blueprint: Callable[..., Any]
    logger: Any
    getPortals: Callable[[], Any]
    getSettings: Callable[[], Any]
    get_db_connection: Callable[[], Any]
    moveMac: Callable[..., Any]
    score_mac_for_selection: Callable[..., Any]
    occupied: Any
    hls_manager: Any


@dataclass
class RuntimeState:
    logger: Any
    job_manager: Any
    scheduler: SchedulerState
    settings: SettingsState
    epg: EpgState
    portal: PortalState
    editor: EditorState
    misc: MiscState
    hdhr: HdhrState
    playlist: PlaylistState
    streaming: StreamingState
