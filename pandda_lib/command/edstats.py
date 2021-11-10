import subprocess

import gemmi


class EDSTATS:
    def __init__(self, input_mtz_file, input_pdb_file):
        mtz = gemmi.read_mtz_file(str(input_mtz_file))

        res_low = mtz.resolution_low()
        res_high = mtz.resolution_high()

        self.command = (
            f"#!/bin/tcsh\n"
            f"rm -f fixed.mtz\n"
            f"mtzfix  FLABEL FP  HKLIN {input_mtz_file}  HKLOUT fixed.mtz  >mtzfix.log\n"
            f"if($?) exit $?\n"
            # Display the log
            f"less mtzfix.log\n"
            # CHoose which mtz to use
            f"if(! -e fixed.mtz)  ln -s  in.mtz fixed.mtz\n"
            # FFT the 2FoFc map
            f"echo 'labi F1=FWT PHI=PHWT\nxyzl asu\ngrid samp 4.5'  | fft HKLIN fixed.mtz  MAPOUT fo.map\n"
            f"if($?) exit $?\n"
            # FFT The FoFc map
            f"echo 'labi F1=DELFWT PHI=PHDELWT\nxyzl asu\ngrid samp 4.5'  | fft  HKLIN fixed.mtz  MAPOUT df.map\n"
            # Do the edstats
            f"echo resl={res_low},resh={res_high}  | edstats  XYZIN {input_pdb_file}  MAPIN1 fo.map  MAPIN2 df.map  QQDOUT q-q.out  OUT stats.out\n"
        )

    def run(self):
        p = subprocess.Popen(
            self.command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        print(stdout)
        print(stderr)
