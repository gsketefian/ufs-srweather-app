# These are the steps needed to set up the conda environment for running
# the SRW workflow.  Source this script from your terminal to set up the
# proper environment in your terminal for generating and running SRW workflows.
# Running this file as a script won't work because it would set up the
# conda environment in a shell that will be closed once this script stops
# executing.  You have to source it.

source etc/lmod-setup.sh hera
cd modulefiles/
module use $(pwd)
module load wflow_hera
conda activate srw_app
cd ..
