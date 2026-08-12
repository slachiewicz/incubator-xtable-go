#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Apache XTable (Go) Python Client.
Provides lightweight, zero-JVM lakehouse metadata translation across Delta, Iceberg, and Hudi.
"""

import ctypes
import json
import os
import platform
from typing import Any, Dict, Union

__version__ = "0.1.0"


def _find_library() -> str:
    system = platform.system()
    if system == "Darwin":
        libname = "libxtable.dylib"
    elif system == "Windows":
        libname = "libxtable.dll"
    else:
        libname = "libxtable.so"

    # Search in common locations
    base_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(base_dir, libname),
        os.path.join(base_dir, "..", "..", "lib", libname),
        os.path.join(base_dir, "..", "lib", libname),
        os.path.join(os.getcwd(), "lib", libname),
    ]

    for path in candidates:
        if os.path.exists(path):
            return path

    return libname


_lib_path = _find_library()
_lib_error = None
try:
    _lib = ctypes.CDLL(_lib_path)

    _lib.xtable_version.restype = ctypes.c_char_p
    _lib.xtable_version.argtypes = []

    _lib.xtable_free_string.restype = None
    _lib.xtable_free_string.argtypes = [ctypes.c_char_p]

    _lib.xtable_sync_json.restype = ctypes.c_char_p
    _lib.xtable_sync_json.argtypes = [ctypes.c_char_p]

    _lib.xtable_inspect_json.restype = ctypes.c_char_p
    _lib.xtable_inspect_json.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
except Exception as e:
    _lib = None
    _lib_error = str(e)


def version() -> str:
    """Returns the native XTable engine version."""
    if _lib is None:
        error_msg = f"{__version__} (shared library not loaded)"
        if _lib_error:
            error_msg += f" - {_lib_error}"
        return error_msg
    res_ptr = _lib.xtable_version()
    val = res_ptr.decode("utf-8") if res_ptr else ""
    return val


def sync(config: Union[Dict[str, Any], str]) -> Dict[str, Any]:
    """
    Synchronizes lakehouse table formats using the provided configuration dict or JSON/YAML string.
    """
    if _lib is None:
        error_msg = "libxtable shared library is not loaded"
        if _lib_error:
            error_msg += f" - {_lib_error}"
        raise RuntimeError(error_msg)

    if isinstance(config, dict):
        config_str = json.dumps(config)
    else:
        config_str = str(config)

    res_ptr = _lib.xtable_sync_json(config_str.encode("utf-8"))
    if not res_ptr:
        raise RuntimeError("Null response from native xtable_sync_json")

    try:
        raw_res = ctypes.cast(res_ptr, ctypes.c_char_p).value.decode("utf-8")
        parsed = json.loads(raw_res)
        if parsed.get("status") == "ERROR":
            raise RuntimeError(parsed.get("error", "Unknown sync error"))
        return parsed
    finally:
        _lib.xtable_free_string(res_ptr)


def inspect(format_name: str, base_path: str) -> Dict[str, Any]:
    """
    Inspects lakehouse table metadata, schema, and active files.
    """
    if _lib is None:
        error_msg = "libxtable shared library is not loaded"
        if _lib_error:
            error_msg += f" - {_lib_error}"
        raise RuntimeError(error_msg)

    res_ptr = _lib.xtable_inspect_json(format_name.encode("utf-8"), base_path.encode("utf-8"))
    if not res_ptr:
        raise RuntimeError("Null response from native xtable_inspect_json")

    try:
        raw_res = ctypes.cast(res_ptr, ctypes.c_char_p).value.decode("utf-8")
        parsed = json.loads(raw_res)
        if parsed.get("status") == "ERROR":
            raise RuntimeError(parsed.get("error", "Unknown inspect error"))
        return parsed
    finally:
        _lib.xtable_free_string(res_ptr)
