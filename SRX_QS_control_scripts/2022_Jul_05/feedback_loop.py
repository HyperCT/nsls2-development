import pprint
import numpy as np
import time as ttime
import gc
from scipy.ndimage import center_of_mass

from databroker import Broker
db = Broker.named("srx")

from bluesky_queueserver_api.zmq import REManagerAPI
from bluesky_queueserver_api import BPlan


# Calculate the center of mass
def calc_com(run_start_uid, roi=None):
    print('Centering sample using center of mass...')

    # Get the header
    h = db[run_start_uid]
    scan_doc = h.start['scan']

    # Get scan parameters
    [x0, x1, nx, y0, y1, ny, dt] = scan_doc['scan_input']

    # Get the data
    flag_get_data = True
    t0 = ttime.monotonic()
    TMAX = 120  # wait a maximum of 60 seconds
    while flag_get_data:
        try:
            d = list(h.data('fluor', stream_name='stream0', fill=True))
            d = np.array(d)
            d_I0 = list(h.data('i0', stream_name='stream0', fill=True))
            d_I0 = np.array(d_I0)
            flag_get_data = False
        except:
            # yield from bps.sleep(1)
            if (ttime.monotonic() - t0 > TMAX):
                print('Data collection timed out!')
                print('Skipping center-of-mass correction...')
                return x0, x1, y0, y1
    # HACK to make sure we clear the cache.  The cache size is 1024 so
    # this would eventually clear, however on this system the maximum
    # number of open files is 1024 so we fail from resource exaustion before
    # we evict anything.
    db._catalog._entries.cache_clear()
    gc.collect()

    # # Setup ROI
    # if (roi is None):
    #     # NEED TO CONFIRM VALUES!
    #     roi = [xs.channel1.rois.roi01.bin_low.get(), xs.channel1.rois.roi01.bin_high.get()]
    #     # NEED TO CONFIRM!
    #     # JL this is close but not quite right
    #     roi = [
    #         mcaroi.min_x.get()
    #         for mcaroi
    #         in xs.channels.channel01.iterate_mcarois()
    #     ]

    #     # By default, do both low/high values reset to zero?
    #     if (roi[1] == 0):
    #         roi[1] = 4096

    d = np.sum(d[:, :, :, roi[0]:roi[1]], axis=(2, 3))
    d = d / d_I0
    d = d.T

    # Calculate center of mass
    if (scan_doc['fast_axis']['motor_name'] == 'nano_stage_sx'):
        (com_x, com_y)  = center_of_mass(d)  # for flying x scans
    elif (scan_doc['fast_axis']['motor_name'] == 'nano_stage_sy'):
        (com_y, com_x)  = center_of_mass(d)  # for y scans
    else:
        print('Not sure how data is oriented. Skipping...')
        return x0, x1, y0, y1
    com_x = x0 + com_x * (x1 - x0) / nx
    com_y = y0 + com_y * (y1 - y0) / ny
    # print(f'Center of mass X: {com_x}')
    # print(f'Center of mass Y: {com_y}')

    # Calculate new center
    extentX = x1 - x0
    old_center = x0 + 0.5 * extentX
    dx = old_center - com_x
    extentY = y1 - y0
    old_center_y = y0 + 0.5 * extentY
    dy = old_center_y - com_y

    # Check new location
    THRESHOLD = 0.50 * extentX
    if np.isfinite(com_x) is False:
        print('Center of mass is not finite!')
        new_center = old_center
    elif np.abs(dx) > THRESHOLD:
        print('New scan center above threshold')
        new_center = old_center
    else:
        new_center = com_x
    x0 = new_center - 0.5 * extentX
    x1 = new_center + 0.5 * extentX
    print(f'Old center: {old_center:.4f}')
    print(f'New center: {new_center:.4f}')
    print(f'  Difference: {dx:.4f}')

    THRESHOLD = 0.50 * extentY
    if np.isfinite(com_y) is False:
        print('Center of mass is not finite!')
        new_center_y = old_center_y
    elif np.abs(dy) > THRESHOLD:
        print('New scan center above threshold')
        new_center_y = old_center_y
    else:
        new_center_y = com_y
    y0 = new_center_y - 0.5 * extentY
    y1 = new_center_y + 0.5 * extentY
    print(f'Old center: {old_center_y:.4f}')
    print(f'New center: {new_center_y:.4f}')
    print(f'  Difference: {dy:.4f}')

    return x0, x1, y0, y1


RM = REManagerAPI()

# theta_list = [0, 45, 90, 135, 180]
th = np.linspace(0, 170, 18)
theta_list = np.concatenate((th, np.array([180]), th+185, th+362.5, th+547.5))
theta_to_start = 195
theta_index_to_start = [int(round(_ * 1000)) for _ in theta_list].index(int(round(theta_to_start * 1000)))
theta_list = theta_list[theta_index_to_start:]
shutters = True
# xstart, xstop, xnum, ystart, ystop, ynum, dwell = -30, 30, 121, -20, 20, 81, 0.050
xstart, xstop, xnum, ystart, ystop, ynum, dwell = -39.503, 20.496, 121, -18.528, 21.472, 81, 0.05
extra_dets = None # ["det2"]  # May be 'None'

# try:
#     RM.environment_close()
#     RM.wait_for_idle()
# except RM.RequestFailedError:
#     pass

# Attempt to open the environement if it does not exist
status = RM.status()
if not status["worker_environment_exists"]:
    RM.environment_open()
    RM.wait_for_idle()

status = RM.status()
if not status["worker_environment_exists"]:
    raise RuntimeError("Failed to open the environment")


def check_plan_result(item_uid, msg):
    item = RM.history_get()["items"][-1]
    item_uid_hist = item["item_uid"]
    if item_uid_hist != item_uid:
        s = f"Item UID of the started ({item_uid}) and completed ({item_uid_hist}) plans do not match"
        s = f"{msg}: {s}"
        print(s)
        raise RuntimeError(s)
    result = item["result"]
    if result["exit_status"] != "completed":
        print(f"{msg} (status: {result['exit_status']!r}): {result['msg']}\n{result['traceback']}")
        raise RuntimeError(f"Plan exit status: {result['exit_status']!r}" + result["msg"])


try:
    # Open shutters
    print("Opening shutters ...")
    response = RM.item_execute(BPlan("check_shutters", shutters, "Open"))
    item_uid = response["item"]["item_uid"]
    RM.wait_for_idle()
    check_plan_result(item_uid, "Failed to open shutters")
    print("Shutters are open.")

    for theta in theta_list:
        # if theta >= -47.9 and theta <= -35.1:
        #     print("=========================================================================")
        #     print(f"            Skipping scan with THETA={theta}")
        #     print("=========================================================================")
        #     continue

        print("=========================================================================")
        print(f"            New projection: THETA={theta}")
        print("=========================================================================")

        # Rotate the stage
        print(f"Rotating the stage to THETA={theta} degrees ...")
        response = RM.item_execute(BPlan("mv", "nano_stage.th", theta * 1000))
        item_uid = response["item"]["item_uid"]
        RM.wait_for_idle()
        check_plan_result(item_uid, "Failed to rotate the stage")
        print("Rotation is completed.")

        # Rotate the stage
        print(f"Scanning the projection ...")
        response = RM.item_execute(
            BPlan(
                "nano_scan_and_fly",
                xstart,
                xstop,
                xnum,
                ystart,
                ystop,
                ynum,
                dwell,
                extra_dets=extra_dets,
                shutter=False,
            )
        )
        item_uid = response["item"]["item_uid"]
        while(1):  # Inifinite wait
            try:
                RM.wait_for_idle(timeout=60)
                break
            except REManagerAPI.WaitTimeoutError:
                pass
        check_plan_result(item_uid, "Projection scan failed")
        print("Scan is completed.")

        item = RM.history_get()["items"][-1]
        run_uid = item["result"]["run_uids"][0]
        print(f"Computing center of mass. Run UID: {run_uid}")
        xstart, xstop, ystart, ystop = calc_com(run_uid, roi=(737, 757))
        print(f"New range: {xstart}, {xstop}, {ystart}, {ystop}")

except Exception:
    print(f"Scan sequence was stopped or failed.")
    raise

except BaseException:
    print("The script was stopped (probably using Ctrl-C). The last plan may still be running.")

finally:
    status = RM.status()
    # print(pprint.pformat(status))
    if status["manager_state"] != "idle":
        print("RE Manager is not IDLE. Shutter can not be closed. Stop the plan and close the shutters manually")
    else:
        # Close shutters
        print("Waiting for RE Manager to switch to IDLE state before shutters could be closed ...")
        while(1):  # Inifinite wait
            try:
                RM.wait_for_idle(timeout=60)
                break
            except REManagerAPI.WaitTimeoutError:
                pass
        print("Closing shutters ...")
        response = RM.item_execute(BPlan("check_shutters", shutters, "Close"))
        item_uid = response["item"]["item_uid"]
        RM.wait_for_idle()
        check_plan_result(item_uid, "Failed to close shutters")
        print("Shutters are closed.")
