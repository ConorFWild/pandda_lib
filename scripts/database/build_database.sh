#!/bin/sh

__conda_setup="$('/dls/science/groups/i04-1/conor_dev/conda/anaconda/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/dls/science/groups/i04-1/conor_dev/conda/anaconda/etc/profile.d/conda.sh" ]; then
        . "/dls/science/groups/i04-1/conor_dev/conda/anaconda/etc/profile.d/conda.sh"
    else
        export PATH="/dls/science/groups/i04-1/conor_dev/conda/anaconda/bin:$PATH"
    fi
fi
unset __conda_setup
export PYTHONPATH=""
conda activate lab

python diamond_build_database_all.py step=4