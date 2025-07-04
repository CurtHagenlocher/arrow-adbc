#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e
set -x
set -o pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <adbc-src-dir>"
  exit 1
fi

source_dir=${1}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${script_dir}/python_util.sh"

echo "=== (${PYTHON_VERSION}) Installing wheels ==="
for component in ${COMPONENTS}; do
    if [[ "${component}" = "adbc_driver_manager" ]]; then
        PYTHON_TAG=cp$(python -c "import sysconfig; print(sysconfig.get_python_version().replace('.', ''))")
        # We only want cp313-cp313 and not cp313-cp313t (for example)
        PYTHON_TAG="${PYTHON_TAG}-${PYTHON_TAG}"
    else
        PYTHON_TAG=py3-none
    fi

    if [[ -d ${source_dir}/python/${component}/repaired_wheels/ ]]; then
        pip install --no-deps --force-reinstall \
            ${source_dir}/python/${component}/repaired_wheels/*-${PYTHON_TAG}-*.whl
    elif [[ -d ${source_dir}/python/${component}/dist/ ]]; then
        pip install --no-deps --force-reinstall \
            ${source_dir}/python/${component}/dist/*-${PYTHON_TAG}-*.whl
    else
        echo "NOTE: assuming wheels are already installed"
    fi
done
pip install importlib-resources pytest pyarrow pandas polars protobuf


echo "=== (${PYTHON_VERSION}) Testing wheels ==="
test_packages

echo "=== (${PYTHON_VERSION}) Testing wheels (no PyArrow) ==="
pip uninstall -y pyarrow
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS} -k pyarrowless"
test_packages_pyarrowless
