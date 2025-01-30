Upon a fresh "git clone https://github.com/E3SM-Project/datasm.git", certain actions must be taken
to allow the the DSM system to function:

1.  Edit the file (git repo)/datasm/datasm/"Relocation/dsm_root_paths" to suit your local file system.
    Note that the choices for
        ARCHIVE_STORAGE
        STAGING_DATA
    must be able to accommodate 50-100TB of space during routine operations.

    Ensure that your choice for "DSM_STAGING" (let's call it <staging>) exists.

2.  Run the script:

        ./Install_Path_Management.sh <staging> <updated_dsm_root_paths_file>

3.  Ensure that the line:

       export DSM_GETPATH=<location>/Relocation/.dsm_get_root_path.sh 

    appears in your .bashrc file.  (The above script will return the precise line).

    Ensure that the command "$DSM_GETPATH ALL" reflects all of the paths you have chosen.

4.  Run the script (in the resources directory):

        (git repo)/datasm/datsm/resource/datasm_config.template.sh > datasm_config.yaml

5.  Run the scripti (in the tools directory):

        (git repo)/datasm/datsm/tools/local_install.sh

    



