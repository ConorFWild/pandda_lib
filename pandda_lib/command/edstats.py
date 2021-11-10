import subprocess

import pandas as pd
import gemmi


class EDSTATS:
    def __init__(self, input_mtz_file, input_pdb_file, f="FWT", phi="PHWT", delta_f="DELFWT", delta_phi="PHDELWT",
                 fix=True):
        mtz = gemmi.read_mtz_file(str(input_mtz_file))

        res_low = mtz.resolution_low()
        res_high = mtz.resolution_high()

        if fix:

            self.command = (
                # f"#!/bin/tcsh\n"
                f"rm -f fixed.mtz\n"
                # f"mtzfix  FLABEL FP  HKLIN {input_mtz_file}  HKLOUT fixed.mtz  >mtzfix.log\n"
                f"mtzfix HKLIN {input_mtz_file} HKLOUT fixed.mtz >mtzfix.log\n"
                f"if($?) exit $?\n"
                # Display the log
                f"less mtzfix.log\n"
                # CHoose which mtz to use
                f"if(! -e fixed.mtz)  ln -s  in.mtz fixed.mtz\n"
                # FFT the 2FoFc map
                f"echo 'labi F1={f} PHI={phi}\\nxyzl asu\\ngrid samp 4.5' | fft HKLIN fixed.mtz MAPOUT fo.map\n"
                f"if($?) exit $?\n"
                # FFT The FoFc map
                f"echo 'labi F1={delta_f} PHI={delta_phi}\\nxyzl asu\\ngrid samp 4.5' | fft HKLIN fixed.mtz MAPOUT df.map\n"
                # Do the edstats
                f"echo resl={res_low},resh={res_high}  | edstats  XYZIN {input_pdb_file} MAPIN1 fo.map MAPIN2 df.map QQDOUT q-q.out OUT stats.out\n"
            )
        else:
            self.command = (
                # FFT the 2FoFc map
                f"echo 'labi F1={f} PHI={phi}\\nxyzl asu\\ngrid samp 4.5' | fft HKLIN {input_mtz_file} MAPOUT fo.map\n"
                f"if($?) exit $?\n"
                # FFT The FoFc map
                f"echo 'labi F1={delta_f} PHI={delta_phi}\\nxyzl asu\\ngrid samp 4.5' | fft HKLIN {input_mtz_file} MAPOUT df.map\n"
                # Do the edstats
                f"echo resl={res_low},resh={res_high}  | edstats  XYZIN {input_pdb_file} MAPIN1 fo.map MAPIN2 df.map QQDOUT q-q.out OUT stats.out\n"
            )


    def run(self):
        # print(self.command)
        p = subprocess.Popen(
            self.command,
            shell=True,
            executable='/bin/tcsh',
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = p.communicate()
        # print(stdout)
        print(stderr)

        table = pd.read_csv('stats.out', delim_whitespace=True)
        """
        4. BAm:   Weighted average Biso.
 5. NPm:   No of statistically independent grid points covered by atoms.
 6. Rm:    Real-space R factor (RSR).
 7. RGm:   Real-space RG factor (RSRG).
 8. SRGm:  Standard uncertainty of RSRG.
 9. CCSm:  Real-space sample correlation coefficient (RSCC).
10. CCPm:  Real-space 'population' correlation coefficient (RSPCC).
11. ZCCPm: Z-score of real-space correlation coefficient.
12. ZOm:   Real-space Zobs score (RSZO).
13. ZDm:   Real-space Zdiff score (RSZD) i.e. max(-RSZD-,RSZD+).
14. ZD-m:  Real-space Zdiff score for negative differences (RSZD-).
15. ZD+m:  Real-space Zdiff score for positive differences (RSZD+).
"""

        stats = table.iloc[:, 27:39]
        rsccs = table['CCSa']

        return rsccs


