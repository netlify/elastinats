#!/usr/bin/env bash

#!/usr/bin/env bash

set -e
set -x

OUTPUT="$1"

make deps
make test
if [ "$OUTPUT" != "" ]; then
    make build
    echo "Finshed building $OUTPUT: $(./$OUTPUT version)"
fi
