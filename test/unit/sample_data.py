import shutil
import os
import datetime

HERE = os.path.abspath(os.path.dirname(__file__))

def generate_rotated_raster(dst, rotated=True):
    tstamp = datetime.datetime.now().isoformat()
    hdr_dst = os.path.join(dst, 'test_vnir_{}.hdr'.format(tstamp))
    img_dst = os.path.join(dst, 'test_vnir_{}.img'.format(tstamp))
    shutil.copy(os.path.join(HERE, 'test.img'), img_dst)
    if rotated:
        shutil.copy(os.path.join(HERE, 'test_rotated.hdr'), hdr_dst)
    else:
        shutil.copy(os.path.join(HERE, 'test_unrotated.hdr'), hdr_dst)

    return img_dst
