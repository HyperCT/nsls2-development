import os
import glob
import time as ttime
from datetime import datetime
import xrf_tomo
import shutil
from pystackreg import StackReg
import numpy as np

raw_data_dir = "raw_data"
proc_data_dir = "proc_data"
param_dir = "."
param_fn = "pyxrf_model_parameters_133727.json"

polling_interval = 1

raw_data_dir = os.path.abspath(os.path.expanduser(raw_data_dir))
proc_data_dir = os.path.abspath(os.path.expanduser(proc_data_dir))
log_dir = proc_data_dir
param_dir = os.path.abspath(os.path.expanduser(param_dir))


def reconstruct(*, recon_alg, raw_data_dir, recon_dir):

    # Create single data file based on the list of projections
    fn_single = os.path.join(recon_dir, "tomo.h5")
    fn_log = os.path.join(recon_dir, "tomo_info.dat")

    # trim_vertical = (30, 100)
    # trim_vertical = (0, 20)
    trim_vertical = (None, None)
    xrf_tomo.make_single_hdf(
        fn_single,
        fn_log=fn_log,
        wd_src=raw_data_dir,
        include_raw_data=False,
        trim_vertical=trim_vertical,
    )

    xrf_tomo.normalize_projections(fn=fn_single, path=recon_dir)
    # xrf_tomo.normalize_projections(fn=fn_single, path=recon_dir, normalize_by_element="Ar_K")

    xrf_tomo.normalize_pixel_range(fn=fn_single, path=recon_dir, read_only=False)

    # el_align = "Ca_K"
    el_align = "Ni_K"
    # el_align = "Co_K"
    # el_align = "total_cnt"

    # for _ in range(1):
    #     xrf_tomo.align_projections_pystackreg(fn=fn_single, el=el_align, path=recon_dir, reverse=False)
    #     xrf_tomo.align_projections_pystackreg(fn=fn_single, el=el_align, path=recon_dir, reverse=True)

    xrf_tomo.align_proj_com(fn=fn_single, el=el_align, path=recon_dir)
    xrf_tomo.shift_projections(fn=fn_single, path=recon_dir, read_only=False)

    # algorithm = "sirt"
    algorithm = "gridrec"
    # algorithm = "mlem"
    # algorithm = "tv"

    # alignment_algorithm = "align_seq"
    # alignment_algorithm = "align_joint"
    alignment_algorithm = "align_com"

    center = None
    # center = 35
    # center=100

    # iters = 100
    # xrf_tomo.find_alignment(
    #     fn=fn_single,
    #     el=el_align,
    #     path=recon_dir,
    #     iters=iters,
    #     center=center,
    #     algorithm=algorithm,
    #     alignment_algorithm=alignment_algorithm,
    #     save=True,
    # )
    # xrf_tomo.shift_projections(fn=fn_single, path=recon_dir, read_only=False)

    xrf_tomo.find_center(fn=fn_single, el=el_align, path=recon_dir)

    rotation_center = None
    # rotation_center = 96
    # rotation_center = 35
    # center_offset = None
    center_offset = -0.5
    # center_offset = -8
    # center_offset = 0

    print("============================================================")
    print(f"  Starting reconstruction")
    if recon_alg == "svmbir":
        xrf_tomo.make_volume_svmbir(
            fn=fn_single, path=recon_dir, center_offset=center_offset
        )
    elif recon_alg in ("fbp", "gridrec"):
        xrf_tomo.make_volume(
            fn=fn_single,
            path=recon_dir,
            algorithm=recon_alg,
            rotation_center=rotation_center,
        )
    else:
        raise RuntimeError(f"Unsupported reconstruction algorithm: {recon_alg}")
    print(f"   Reconstruction finished")
    print("=============================================================")

    xrf_tomo.export_tiff_projs(
        fn=fn_single,
        fn_dir=recon_dir,
        tiff_dir=recon_dir,
        raw=False,
    )
    xrf_tomo.export_tiff_volumes(fn=fn_single, fn_dir=recon_dir, tiff_dir=recon_dir)

    # Remove 'single' file (to save space)
    os.remove(os.path.join(recon_dir, fn_single))


def run_processing():

    n_processed_files = 0

    while True:

        raw_data_files = glob.glob(os.path.join(raw_data_dir, "*.h5"))
        raw_data_files.sort()

        if len(raw_data_files) > n_processed_files:
            ttime.sleep(30)
            print(f"Number of available data files: {len(raw_data_files)}")

            # Copy the new files to the directory with processed data
            for fn in raw_data_files:
                fn_dest = os.path.join(proc_data_dir, os.path.basename(fn))
                if not os.path.isfile(fn_dest):
                    shutil.copy(fn, fn_dest)

            # Create the list of files in the directory with processed data. It is expected to be the same
            #   as 'raw_data_files', but it could be different.
            proc_data_files = glob.glob(os.path.join(proc_data_dir, "*.h5"))
            proc_data_files.sort()

            n_processed_files = len(proc_data_files)

            log_path = os.path.join(log_dir, "tomo_info.dat")
            param_path = os.path.join(param_dir, param_fn)

            xrf_tomo.create_log_file(fn_log=log_path, wd=log_dir)

            # Processed only unprocessed (new) files
            xrf_tomo.process_proj(
                wd=proc_data_dir,
                fn_param=param_path,
                fn_log=log_path,
                ic_name="i0",
                save_tiff=False,
                skip_processed=True,
            )

            if n_processed_files < 2:
                continue

            # algorithms = ["svmbir", "gridrec"]
            algorithms = ["svmbir"]
            # algorithms = ["gridrec"]

            for alg in algorithms:
                time_str = datetime.now().strftime(r"%Y%m%d-%H%M%S")
                recon_dir = f"{alg}-{time_str}-{n_processed_files:03d}"
                recon_dir = os.path.join(proc_data_dir, recon_dir)
                os.makedirs(recon_dir, exist_ok=True)
                shutil.copy(log_path, recon_dir)

                reconstruct(
                    recon_alg=alg, raw_data_dir=proc_data_dir, recon_dir=recon_dir
                )

        else:
            ttime.sleep(polling_interval)


if __name__ == "__main__":
    run_processing()
