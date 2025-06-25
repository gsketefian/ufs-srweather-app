##KE Spectra code
##Written by Maria Frediani, NSF NCAR / RAL, 2022
##Modifications by Will Mayfield, 2024

import os
import sys
import numpy as np
import matplotlib.pyplot as plt
import matplotlib as mpl
from matplotlib.cm import get_cmap

from marias_ke_error_functions import *

# px = 'p1'  # Perturbed experiment
# px0 = 'p0'  # CTRL experiment


##choose levels, forecast hours, and experiments##

#levs = [63, 57, 51, 45, 39, 33, 27] 
levs = [63,48]
#fhrs = [1,3,6,12,24,36]
fhrs = [1,12]
#expts = ['ALL_SPP']
#expts = ['SPP_SPPT_SHUM_SKEB','SPPT_SHUM_SKEB','SPPT','SKEB','SHUM','ALL_SPP','MYNN_PBL','MYNN_SFC','MP_GT_4','MP_GT_6','MP','GSL_GWD','RRTMG_RAD']
expts = ['SPP_SPPT_SHUM_SKEB','ALL_SPP','SPPT']

##some plotting settings##

#snsc = get_cmap('Set2')
snsc = get_cmap('tab10')
#lw = 1.25
lwidth = 1.8
ls = ['-', '--', ':', '-.']

#this corresponds to the dimensions
kmax = 1015
k = np.arange(kmax + 1)[2::2]  # 2, 4, 6, ..., 1020
kmx = 505

for lev in levs:#separate plots per level
    for idx2,fhr in enumerate(fhrs):#separate plot for forecast hours

        #set main plot title ---
        title = f'fhr{fhr:02} 3km CONUS Kinetic Energy Error 2019-06-15 lev{lev:02}'
        ax, tt = start_plot(k[kmx], title=title)

        for idx1,expt in enumerate(expts):#experiments on same plot
            # Ctrl simulation:
            wrffile = f'/glade/work/wmayfield/hiwt/error-spectra-will/sample_data/NO_SPP/dynf0{fhr:02}.nc'
            # Perturbed simulation:
            wrffile1 = f'/glade/work/wmayfield/hiwt/error-spectra-will/sample_data/{expt}/dynf0{fhr:02}.nc'

            # --------------------------------------------------------
            # --------------------------------------------------------
            # Load wrf data forecast CTRL

            u_ctrl = read_wrf_fcst(var='ugrd', lev=lev, file_set=wrffile)
            v_ctrl = read_wrf_fcst(var='vgrd', lev=lev, file_set=wrffile)
            u_ctrl[u_ctrl == -999.0] = np.nan
            v_ctrl[v_ctrl == -999.0] = np.nan
            # --------------------------------------------------------

            # --------------------------------------------------------
            # Load wrf data forecast for experiment1

            u_exp = read_wrf_fcst(var='ugrd', lev=lev, file_set=wrffile1)
            v_exp = read_wrf_fcst(var='vgrd', lev=lev, file_set=wrffile1)
            u_exp[u_exp == -999.0] = np.nan
            v_exp[v_exp == -999.0] = np.nan
            # --------------------------------------------------------


            # --------------------------------------------------------
            # This is a quick fix to make dimensions match...
            # wrf's unprocessed outputs generate staggered dimenions.
            # The way to do it is to unstagger them, but I'm only trying to
            # make the code run.
            # I don't have the unstaggered files at pressure levels anymore

            #u_ctrl = u_ctrl[:, :-1]
            #u_exp = u_exp[:, :-1]
            #v_ctrl = v_ctrl[:-1, :]
            #v_exp = v_exp[:-1, :]

            # --------------------------------------------------------
            # Calculate and save the Error Energy

            outfile = './output/eKE_20190615_' + f'{fhr:02}' + 'Z_lev' + str(lev) + f'_{expt}'
            path_eKE = os.path.exists(outfile+'.npz')

            if path_eKE:
                print('data exists, loading from '+outfile)
                eKE = load_data(outfile+'.npz', var='eKE')
            else:
                print('data does not exist, saving...')
                eKE = save_eKE(u_ctrl=u_ctrl,
                               v_ctrl=v_ctrl,
                               u_exp=u_exp,
                               v_exp=v_exp,
                               outfile=outfile)

            # --------------------------------------------------------
            # Plot

            # I'm multiplying eKE by a scalar because
            # there's not enough error energy yet to appear on the plot...
            eKEp1 = eKE

            # Set up the periodic domain
            # This follows from the paper Denis et al 2002 (MWR) and Durran et al 2017 (MWR)
            eKE_kp1 = eKEp1[:, 1:-2:2] + eKEp1[:, 2:-1:2]

            # --------------------------------------------------------
            # --------------------------------------------------------

            il = 0

            #use something like this to bolden/highlight certain lines being plotted
            if idx1 < 6:
                lw = lwidth
            else:
                #lw = lwidth*3
                lw = lwidth

            ax.plot(k[:kmx + 1],
                    eKE_kp1[il, :kmx + 1],
                    lw=lw,
                    ls=ls[0],
                    color=snsc.colors[idx1],
                    label=f'{expt} {fhr:02}h')

        # --------------------------------------------------------
        fname = 'ErrEnergy_ALL_' + f'fhr{fhr:02}_2019-06-15_lev' + str(lev)
        ax.legend(loc='lower left',prop={'size': 11})
        plt.savefig('./plots/' + fname + '.png', bbox_inches='tight', dpi=100)
        plt.close()
