import os
import subprocess
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from flask import Blueprint, Response, make_response, redirect, request, send_file

import stb


def create_streaming_blueprint(
    *,
    logger,
    getPortals,
    getSettings,
    get_db_connection,
    moveMac,
    score_mac_for_selection,
    occupied,
    hls_manager,
):
    bp = Blueprint("streaming", __name__)

    @bp.route("/play/<portalId>/<channelId>", methods=["GET"])
    def channel(portalId, channelId):
        def streamData():
            ffmpeg_sp = None
            occupied_item = None
            stderr_buffer = []
            stderr_lock = threading.Lock()

            def _drain_stderr(pipe):
                try:
                    for line in iter(pipe.readline, b""):
                        text = line.decode(errors="ignore").strip()
                        if not text:
                            continue
                        with stderr_lock:
                            stderr_buffer.append(text)
                            if len(stderr_buffer) > 50:
                                stderr_buffer.pop(0)
                except Exception:
                    pass
            def occupy():
                occupied.setdefault(portalId, [])
                nonlocal occupied_item
                occupied_item = {
                    "mac": mac,
                    "channel id": channelId,
                    "channel name": channelName,
                    "client": ip,
                    "portal name": portalName,
                    "start time": startTime,
                }
                occupied.get(portalId, []).append(occupied_item)
                logger.info(
                    "Occupied Portal({} | {}):MAC({})".format(portalName, portalId, mac)
                )

            def unoccupy():
                try:
                    if occupied_item and occupied_item in occupied.get(portalId, []):
                        occupied.get(portalId, []).remove(occupied_item)
                        logger.info(
                            "Unoccupied Portal({} | {}):MAC({})".format(
                                portalName, portalId, mac
                            )
                        )
                except Exception:
                    pass

            try:
                startTime = datetime.now(timezone.utc).timestamp()
                occupy()
                ffmpeg_sp = subprocess.Popen(
                    ffmpegcmd,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                stderr_thread = threading.Thread(
                    target=_drain_stderr, args=(ffmpeg_sp.stderr,), daemon=True
                )
                stderr_thread.start()
                while True:
                    chunk = ffmpeg_sp.stdout.read(1024)
                    if len(chunk) == 0:
                        rc = ffmpeg_sp.poll()
                        if rc not in (None, 0):
                            logger.info(
                                "Ffmpeg closed with error({}). Moving MAC({}) for Portal({})".format(
                                    str(rc), mac, portalName
                                )
                            )
                            with stderr_lock:
                                if stderr_buffer:
                                    logger.info(
                                        "Ffmpeg stderr tail: %s", " | ".join(stderr_buffer[-8:])
                                    )
                            moveMac(portalId, mac)
                        break
                    yield chunk
            except Exception:
                pass
            finally:
                unoccupy()
                if ffmpeg_sp is not None:
                    try:
                        ffmpeg_sp.kill()
                    except Exception:
                        pass

        def testStream():
            timeout = int(getSettings()["ffmpeg timeout"]) * int(1000000)
            ffprobecmd = ["ffprobe", "-timeout", str(timeout), "-i", link]

            if proxy:
                ffprobecmd.insert(1, "-http_proxy")
                ffprobecmd.insert(2, proxy)

            with subprocess.Popen(
                ffprobecmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ) as ffprobe_sb:
                ffprobe_sb.communicate()
                if ffprobe_sb.returncode == 0:
                    return True
                return False

        def isMacFree():
            count = 0
            for i in occupied.get(portalId, []):
                if i["mac"] == mac:
                    count = count + 1
            if count < streamsPerMac:
                return True
            return False

        portal = getPortals().get(portalId)
        portalName = portal.get("name")
        url = portal.get("url")
        streamsPerMac = int(portal.get("streams per mac"))
        proxy = portal.get("proxy")
        web = request.args.get("web")
        ip = request.remote_addr
        channelName = portal.get("custom channel names", {}).get(channelId)

        available_macs = []
        alternate_ids = []
        cached_cmd = None
        cached_channel_name = None
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT available_macs, alternate_ids, cmd, name FROM channels WHERE portal_id = ? AND channel_id = ?",
                [portalId, channelId],
            )
            row = cursor.fetchone()
            if row:
                if row[0]:
                    available_macs = [m.strip() for m in row[0].split(",") if m.strip()]
                if row[1]:
                    alternate_ids = [aid.strip() for aid in row[1].split(",") if aid.strip()]
                if row[2]:
                    cached_cmd = row[2]
                if row[3]:
                    cached_channel_name = row[3]
            conn.close()
        except Exception as e:
            logger.debug(f"Could not get channel data for channel {channelId}: {e}")

        channel_ids_to_try = [channelId] + alternate_ids
        if alternate_ids:
            logger.debug(f"Channel {channelId} has alternate IDs: {alternate_ids}")

        macs_dict = portal["macs"]
        occupied_list = occupied.get(portalId, [])
        mac_scores = []
        for mac, mac_data in macs_dict.items():
            score = score_mac_for_selection(mac, mac_data, occupied_list, streamsPerMac)
            mac_scores.append((mac, score))

        mac_scores.sort(key=lambda x: (x[1] >= 0, x[1]), reverse=True)
        macs = [m[0] for m in mac_scores if m[1] >= 0] or list(macs_dict.keys())

        if available_macs:
            valid_available = [m for m in macs if m in available_macs]
            other_macs = [m for m in macs if m not in available_macs]
            if valid_available:
                macs = valid_available + other_macs
                logger.debug(
                    f"Prioritizing {len(valid_available)} available MACs for channel {channelId}"
                )

        logger.debug(f"MAC scores for Portal({portalName}): {mac_scores[:5]}")

        logger.info(
            "IP({}) requested Portal({}):Channel({})".format(ip, portalId, channelId)
        )

        def probe_single_mac(mac_to_test):
            try:
                if streamsPerMac != 0 and not isMacFree():
                    return None

                logger.info(
                    "Trying Portal({}):MAC({}):Channel({})".format(
                        portalId, mac_to_test, channelId
                    )
                )

                token = stb.getToken(url, mac_to_test, proxy)
                if not token:
                    return None

                stb.getProfile(url, mac_to_test, token, proxy)

                cmd = None
                found_channel_name = (
                    portal.get("custom channel names", {}).get(channelId)
                    or cached_channel_name
                )

                if cached_cmd:
                    cmd = cached_cmd
                    logger.debug(f"Using cached cmd for channel {channelId}")
                else:
                    logger.debug(f"No cached cmd, fetching all channels for MAC {mac_to_test}")
                    channels = stb.getAllChannels(url, mac_to_test, token, proxy)

                    if not channels:
                        return None

                    used_channel_id = channelId

                    for try_channel_id in channel_ids_to_try:
                        for c in channels:
                            if str(c["id"]) == try_channel_id:
                                if found_channel_name is None:
                                    found_channel_name = c["name"]
                                cmd = c["cmd"]
                                used_channel_id = try_channel_id
                                if try_channel_id != channelId:
                                    logger.info(
                                        f"Using alternate channel ID {try_channel_id} instead of {channelId}"
                                    )
                                break
                        if cmd:
                            break

                if not cmd:
                    return None

                if "http://localhost/" in cmd:
                    link = stb.getLink(url, mac_to_test, token, cmd, proxy)
                else:
                    link = cmd.split(" ")[1]

                if not link:
                    return None

                return {
                    "mac": mac_to_test,
                    "token": token,
                    "link": link,
                    "channelName": found_channel_name,
                }
            except Exception as e:
                logger.error(f"Error probing MAC({mac_to_test}): {e}")
                return None

        freeMac = False
        result = None
        failed_macs = []

        parallel_enabled = getSettings().get("parallel mac probing", False)
        max_workers = int(getSettings().get("parallel mac workers", "3"))

        if parallel_enabled and len(macs) > 1:
            logger.info(
                f"Using parallel MAC probing with {max_workers} workers for {len(macs)} MACs"
            )

            with ThreadPoolExecutor(max_workers=min(max_workers, len(macs))) as executor:
                future_to_mac = {
                    executor.submit(probe_single_mac, mac): mac for mac in macs
                }

                for future in as_completed(future_to_mac):
                    mac = future_to_mac[future]
                    try:
                        probe_result = future.result()
                        if probe_result:
                            result = probe_result
                            freeMac = True
                            for f in future_to_mac:
                                f.cancel()
                            break
                        failed_macs.append(mac)
                    except Exception as e:
                        logger.error(f"Exception probing MAC({mac}): {e}")
                        failed_macs.append(mac)
        else:
            for mac in macs:
                probe_result = probe_single_mac(mac)
                if probe_result:
                    result = probe_result
                    freeMac = True
                    break
                failed_macs.append(mac)
                if not getSettings().get("try all macs", True):
                    break

        for failed_mac in failed_macs:
            logger.info("Moving MAC({}) for Portal({})".format(failed_mac, portalName))
            moveMac(portalId, failed_mac)

        if result:
            mac = result["mac"]
            link = result["link"]
            channelName = result["channelName"]

            if web:
                ffmpegcmd = [
                    "ffmpeg",
                    "-loglevel",
                    "panic",
                    "-hide_banner",
                    "-i",
                    link,
                    "-vcodec",
                    "copy",
                    "-f",
                    "mp4",
                    "-movflags",
                    "frag_keyframe+empty_moov",
                    "pipe:",
                ]
                if proxy:
                    ffmpegcmd.insert(1, "-http_proxy")
                    ffmpegcmd.insert(2, proxy)
                return Response(streamData(), mimetype="application/octet-stream")

            if getSettings().get("stream method", "ffmpeg") == "ffmpeg":
                ffmpegcmd = str(getSettings()["ffmpeg command"])
                ffmpegcmd = ffmpegcmd.replace("<url>", link)
                ffmpegcmd = ffmpegcmd.replace(
                    "<timeout>",
                    str(int(getSettings()["ffmpeg timeout"]) * int(1000000)),
                )
                if proxy:
                    ffmpegcmd = ffmpegcmd.replace("<proxy>", proxy)
                else:
                    ffmpegcmd = ffmpegcmd.replace("-http_proxy <proxy>", "")
                " ".join(ffmpegcmd.split())
                ffmpegcmd = ffmpegcmd.split()
                return Response(streamData(), mimetype="application/octet-stream")

            logger.info("Redirect sent")
            return redirect(link)

        if freeMac:
            logger.info(
                "No working streams found for Portal({}):Channel({})".format(
                    portalId, channelId
                )
            )
        else:
            logger.info(
                "No free MAC for Portal({}):Channel({})".format(portalId, channelId)
            )

        return make_response("No streams available", 503)

    @bp.route("/hls/<portalId>/<channelId>/<path:filename>", methods=["GET"])
    def hls_stream(portalId, channelId, filename):
        portal = getPortals().get(portalId)
        if not portal:
            logger.error(f"Portal {portalId} not found for HLS request")
            return make_response("Portal not found", 404)

        portalName = portal.get("name")
        url = portal.get("url")
        macs = list(portal["macs"].keys())
        proxy = portal.get("proxy")
        ip = request.remote_addr

        logger.info(
            f"HLS request from IP({ip}) for Portal({portalId}):Channel({channelId}):File({filename})"
        )

        stream_key = f"{portalId}_{channelId}"

        stream_exists = stream_key in hls_manager.streams

        if stream_exists:
            logger.debug(
                f"Stream already active for {stream_key}, checking for file: {filename}"
            )
            if filename.endswith(".m3u8"):
                is_passthrough = hls_manager.streams[stream_key].get(
                    "is_passthrough", False
                )
                max_wait = 100 if not is_passthrough else 10
                logger.debug(
                    f"Waiting for {filename} from active stream (passthrough={is_passthrough})"
                )

                for wait_count in range(max_wait):
                    file_path = hls_manager.get_file(portalId, channelId, filename)
                    if file_path:
                        logger.debug(f"File ready after {wait_count * 0.1:.1f}s")
                        break
                    time.sleep(0.1)
            else:
                file_path = hls_manager.get_file(portalId, channelId, filename)
        else:
            logger.debug("Stream not active, will need to start it")
            file_path = None

        if not file_path and (
            filename.endswith(".m3u8")
            or filename.endswith(".ts")
            or filename.endswith(".m4s")
        ):
            logger.debug(
                f"Fetching stream URL for channel {channelId} from portal {portalName}"
            )
            link = None
            for mac in macs:
                try:
                    logger.debug(f"Trying MAC: {mac}")
                    token = stb.getToken(url, mac, proxy)
                    if token:
                        stb.getProfile(url, mac, token, proxy)
                        channels = stb.getAllChannels(url, mac, token, proxy)

                        if channels:
                            for c in channels:
                                if str(c["id"]) == channelId:
                                    cmd = c["cmd"]
                                    if "http://localhost/" in cmd:
                                        link = stb.getLink(url, mac, token, cmd, proxy)
                                    else:
                                        link = cmd.split(" ")[1]
                                    logger.debug(
                                        f"Found stream URL for channel {channelId}"
                                    )
                                    break

                        if link:
                            break
                except Exception as e:
                    logger.error(
                        f"Error getting stream URL for HLS with MAC {mac}: {e}"
                    )
                    continue

            if not link:
                logger.error(
                    f"✗ Could not get stream URL for Portal({portalId}):Channel({channelId}) - tried {len(macs)} MAC(s)"
                )
                return make_response("Stream not available", 503)

            try:
                logger.debug(f"Starting new stream for {stream_key}")
                stream_info = hls_manager.start_stream(portalId, channelId, link, proxy)

                is_passthrough = stream_info.get("is_passthrough", False)

                if filename.endswith(".m3u8"):
                    logger.debug(
                        f"Waiting for playlist file: {filename} (passthrough={is_passthrough})"
                    )
                    max_wait = 100 if not is_passthrough else 10

                    for wait_count in range(max_wait):
                        file_path = hls_manager.get_file(portalId, channelId, filename)
                        if file_path:
                            logger.debug(
                                f"Playlist ready after {wait_count * 0.1:.1f}s"
                            )
                            break
                        time.sleep(0.1)

                    if not file_path:
                        logger.warning(
                            f"Playlist {filename} not ready after {max_wait * 0.1:.0f} seconds"
                        )
                        if not is_passthrough and stream_key in hls_manager.streams:
                            process = hls_manager.streams[stream_key]["process"]
                            if process.poll() is not None:
                                logger.error(
                                    f"FFmpeg crashed during startup (exit code: {process.returncode})"
                                )
                            else:
                                temp_dir = hls_manager.streams[stream_key]["temp_dir"]
                                try:
                                    files = os.listdir(temp_dir)
                                    logger.warning(
                                        f"FFmpeg still running but {filename} not found. Temp dir contains: {files}"
                                    )
                                except Exception as e:
                                    logger.error(f"Could not list temp dir: {e}")
                else:
                    logger.debug(f"Waiting for segment file: {filename}")
                    for wait_count in range(30):
                        file_path = hls_manager.get_file(portalId, channelId, filename)
                        if file_path:
                            logger.debug(
                                f"Segment ready after {wait_count * 0.1:.1f}s"
                            )
                            break
                        time.sleep(0.1)

                    if not file_path:
                        logger.warning(f"Segment {filename} not ready after 3 seconds")

            except Exception as e:
                logger.error(f"✗ Error starting HLS stream: {e}")
                logger.debug(f"Exception details: {type(e).__name__}: {str(e)}")
                return make_response("Error starting stream", 500)

        if file_path and os.path.exists(file_path):
            try:
                if filename.endswith(".m3u8"):
                    mimetype = "application/vnd.apple.mpegurl"
                elif filename.endswith(".ts"):
                    mimetype = "video/mp2t"
                elif filename.endswith(".m4s") or filename.endswith(".mp4"):
                    mimetype = "video/mp4"
                else:
                    mimetype = "application/octet-stream"

                file_size = os.path.getsize(file_path)
                logger.debug(f"Serving {filename} ({file_size} bytes, {mimetype})")

                if filename.endswith(".m3u8") and file_path:
                    try:
                        temp_dir = hls_manager.streams[stream_key]["temp_dir"]
                        available_files = [
                            f
                            for f in os.listdir(temp_dir)
                            if f.endswith(".ts") or f.endswith(".m4s")
                        ]
                        logger.debug(
                            f"Available segments in temp dir: {sorted(available_files)}"
                        )
                    except Exception as e:
                        logger.debug(f"Could not list segments: {e}")

                if filename.endswith(".m3u8") and file_size < 5000:
                    try:
                        with open(file_path, "r") as f:
                            content = f.read()
                            logger.debug(f"Playlist content:\n{content}")
                    except Exception as e:
                        logger.debug(f"Could not read playlist content: {e}")

                return send_file(file_path, mimetype=mimetype)
            except Exception as e:
                logger.error(f"✗ Error serving HLS file {filename}: {e}")
                return make_response("Error serving file", 500)

        logger.warning(f"✗ HLS file not found: {filename} for {stream_key}")
        return make_response("File not found", 404)

    return bp
