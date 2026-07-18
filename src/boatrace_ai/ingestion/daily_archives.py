# Download and extract official BOAT RACE daily archives.
from __future__ import annotations
import datetime as dt
import hashlib
import json
import re
import shutil
import subprocess
from pathlib import Path
import requests

BASE_URL="https://www1.mbrace.or.jp/od2"
USER_AGENT="Mozilla/5.0 BOATRACE-AI personal-research/1.0"
ARCHIVE_TYPES={"result":{"remote_dir":"K","prefix":"k","raw_dir":"results","temporary_dir":"results"},"program":{"remote_dir":"B","prefix":"b","raw_dir":"programs","temporary_dir":"programs"}}

class ArchiveError(RuntimeError):
    pass

def normalize_date(value):
    if isinstance(value,dt.datetime):
        return value.date()
    if isinstance(value,dt.date):
        return value
    return dt.date.fromisoformat(str(value))

def build_archive_spec(race_date,archive_type):
    date_value=normalize_date(race_date)
    if archive_type not in ARCHIVE_TYPES:
        raise ValueError("archive_typeはprogramまたはresultを指定してください")
    config=ARCHIVE_TYPES[archive_type]
    month=date_value.strftime("%Y%m")
    short_date=date_value.strftime("%y%m%d")
    filename="{}{}.lzh".format(config["prefix"],short_date)
    url="{}/{}/{}/{}".format(BASE_URL,config["remote_dir"],month,filename)
    return {"race_date":date_value.isoformat(),"archive_type":archive_type,"filename":filename,"url":url,"raw_dir":config["raw_dir"],"temporary_dir":config["temporary_dir"]}

def sha256_file(path):
    digest=hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda:stream.read(1024*1024),b""):
            digest.update(chunk)
    return digest.hexdigest()

def validate_lzh_file(path):
    file_path=Path(path)
    if not file_path.exists():
        raise ArchiveError("LZHファイルがありません: {}".format(file_path))
    size=file_path.stat().st_size
    if size<=0:
        raise ArchiveError("LZHファイルが空です: {}".format(file_path))
    header=file_path.read_bytes()[:64]
    if re.search(rb"-lh[0-9a-z]-",header,re.IGNORECASE) is None:
        raise ArchiveError("LZHシグネチャを確認できません: {}".format(file_path))
    return {"path":str(file_path),"size":size,"sha256":sha256_file(file_path)}

def download_archive(race_date,archive_type,data_root,overwrite=False,timeout=60,session=None):
    root=Path(data_root)
    spec=build_archive_spec(race_date,archive_type)
    destination=root/"raw"/spec["raw_dir"]/spec["filename"]
    metadata_path=destination.with_suffix(destination.suffix+".json")
    destination.parent.mkdir(parents=True,exist_ok=True)
    if destination.exists() and not overwrite:
        validate_lzh_file(destination)
        return destination
    temporary_path=destination.with_suffix(destination.suffix+".part")
    temporary_path.unlink(missing_ok=True)
    client=session if session is not None else requests
    response=client.get(spec["url"],headers={"User-Agent":USER_AGENT},timeout=timeout,stream=True)
    try:
        response.raise_for_status()
        with temporary_path.open("wb") as stream:
            for chunk in response.iter_content(chunk_size=1024*128):
                if chunk:
                    stream.write(chunk)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    try:
        validation=validate_lzh_file(temporary_path)
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    temporary_path.replace(destination)
    metadata={"race_date":spec["race_date"],"archive_type":archive_type,"url":spec["url"],"path":str(destination),"size":validation["size"],"sha256":sha256_file(destination),"downloaded_at":dt.datetime.now(dt.timezone.utc).isoformat()}
    metadata_path.write_text(json.dumps(metadata,ensure_ascii=False,indent=2),encoding="utf-8")
    return destination

def extract_archive(archive_path,data_root,archive_type,overwrite=False,timeout=120):
    archive=Path(archive_path)
    if archive_type not in ARCHIVE_TYPES:
        raise ValueError("archive_typeはprogramまたはresultを指定してください")
    validate_lzh_file(archive)
    executable=shutil.which("7z")
    if executable is None:
        raise ArchiveError("7zが見つかりません")
    config=ARCHIVE_TYPES[archive_type]
    destination=Path(data_root)/"temporary"/config["temporary_dir"]/archive.stem
    destination.mkdir(parents=True,exist_ok=True)
    existing=sorted(path for path in destination.rglob("*") if path.is_file())
    if existing and not overwrite:
        return existing
    if overwrite:
        for path in existing:
            path.unlink()
    command=[executable,"x",str(archive),"-o{}".format(destination),"-y"]
    result=subprocess.run(command,capture_output=True,text=True,timeout=timeout)
    if result.returncode!=0:
        raise ArchiveError("LZH展開失敗: {} {}".format(result.returncode,result.stderr[-1000:]))
    extracted=sorted(path for path in destination.rglob("*") if path.is_file())
    if not extracted:
        raise ArchiveError("展開ファイルが生成されませんでした: {}".format(archive))
    return extracted

def download_and_extract_daily(race_date,data_root,overwrite=False):
    outputs={}
    for archive_type in ("program","result"):
        archive=download_archive(race_date,archive_type,data_root,overwrite=overwrite)
        files=extract_archive(archive,data_root,archive_type,overwrite=overwrite)
        outputs[archive_type]={"archive":archive,"files":files}
    return outputs
