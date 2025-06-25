##KE Spectra code      
##Written by Maria Frediani, NSF NCAR / RAL, 2022                                                                                      ##Modifications by Will Mayfield, 2024

import os
import numpy as np
from netCDF4 import Dataset
import matplotlib.pyplot as plt
import matplotlib as mpl

# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def load_data(fin, *, var):

    # Load eKE (or KE) previously calculated with save_eKE (or save_KE).
    if not os.path.exists(fin):
        raise Exception("Input file does not exist: " + fin)

    dat = np.load(fin)
    eKE = dat['eKE']
    dat.close()

    return eKE


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def save_eKE(*, u_ctrl, v_ctrl, u_exp, v_exp, outfile=None):

    # Calculate the error Kinetic Energy between two experiments

    from scipy.fftpack import dctn

    ak = None
    bands = None
    eKE = None

    # --------------------------------------------------------
    # --------------------------------------------------------

    # U, V error
    du = u_exp - u_ctrl
    dv = v_exp - v_ctrl

    # --------------------------------------------------------
    # --------------------------------------------------------

    fij = du
    fij[np.isnan(fij)] = 0.0
    duFmn = dctn(fij, type=2, norm='ortho')

    fij = dv
    fij[np.isnan(fij)] = 0.0
    dvFmn = dctn(fij, type=2, norm='ortho')

    if ak is None or bands is None:
        ak, bands = spectral_bands(fij)

    um, du2_mn = spectral_variance(duFmn, bands=bands)
    vm, dv2_mn = spectral_variance(dvFmn, bands=bands)

    if eKE is None:
        eKE = np.expand_dims((du2_mn + dv2_mn) / 2, axis=0)
    else:
        eKE = np.vstack((eKE, (du2_mn + dv2_mn) / 2))

    print(eKE.shape)
    # --------------------------------------------------------
    # --------------------------------------------------------

    if outfile is None:
        outfile = 'saved_errorKE'

    np.savez_compressed(outfile, eKE=eKE)
    return eKE


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def save_KE(*, u, v, outfile=None):

    # Calculate the Kinetic Energy at a given pressure level

    from scipy.fftpack import dctn

    # --------------------------------------------------------
    # --------------------------------------------------------

    ak = None
    bands = None
    KE = None

    # --------------------------------------------------------
    # --------------------------------------------------------

    fij = u
    fij[np.isnan(fij)] = 0.0
    uFmn = dctn(fij, type=2, norm='ortho')

    fij = v
    fij[np.isnan(fij)] = 0.0
    vFmn = dctn(fij, type=2, norm='ortho')

    if ak is None or bands is None:
        ak, bands = spectral_bands(fij)

    um, u2_mn = spectral_variance(uFmn, bands=bands)
    vm, v2_mn = spectral_variance(vFmn, bands=bands)

    if KE is None:
        KE = np.expand_dims((u2_mn + v2_mn) / 2, axis=0)
    else:
        KE = np.vstack((KE, (u2_mn + v2_mn) / 2))

    print(KE.shape)

    # --------------------------------------------------------
    # --------------------------------------------------------

    if outfile is None:
        outfile = 'saved_meanKE'

    np.savez_compressed(outfile, KE=KE)
    return KE


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def spectral_bands(fij):

    # ----------------------------------------------------
    # Fixed values depending only on domain size
    # ----------------------------------------------------

    # Domain index ij
    Ni = fij.shape[0]
    Nj = fij.shape[1]
    N = np.min([Ni, Nj])
    # dx = 3000  # grid spacing in meter

    # ----------------------------------------------------
    # To build the spectrum, the variance contributions of the
    # F(m, n) coefficients need to be binned accordingly to bands of alpha.
    # ----------------------------------------------------

    # 2-D wavenumber indices corresponding to the physical domain grid pts
    #  and to the indices of the DCT array shape
    m, n = np.meshgrid(np.arange(0, Ni), np.arange(0, Nj), indexing='ij')

    alph = np.sqrt((m**2) / (Ni**2) + (n**2) / (Nj**2))
    ak = np.arange(0, N + 1) / N
    bands = alph.copy()
    for kk in np.arange(1, N):
        bands[(alph >= ak[kk]) & (alph < ak[kk + 1])] = kk

    bands[(alph < ak[1])] = 0
    bands[alph >= 1] = 0
    bands[0, 0] = 0
    kmax = np.min([np.max(bands), N])

    return (ak[:int(kmax) + 1], bands)


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def spectral_variance(fmn, *, bands):

    # Based on Denis et al 2002 (MWR) and Durran et al 2017 (MWR)
    # Durran17: "the application of the DCT to aperiodic data is conceptually
    #            equivalent to imposing symmetry boundary conditions at both
    #            edges of the domain, mirroring the existing data across one
    #            symmetry boundary, and applying a conventional FFT on the
    #            expanded domain.

    # fij is the 2D-grided field array

    # ----------------------------------------------------
    # Apply the Discrete Cosine Transform
    # DCT-II norm=ortho
    # y[k] = 2* sum x[n]*cos(pi*k*(2n+1)/(2*N)), 0 <= k < N.
    # f = sqrt(1/(4*N)) if k = 0,
    # f = sqrt(1/(2*N)) otherwise.
    # ----------------------------------------------------

    # from scipy.fftpack import dctn, idctn

    # fij[np.isnan(fij)] = 0.0

    # Fmn = dctn(fij, type=2, norm='ortho')

    Ni = fmn.shape[0]
    Nj = fmn.shape[1]
    N = np.min([Ni, Nj])

    # Spectral Variance
    spec_var = fmn**2 / (Ni * Nj)
    mean = np.sqrt(spec_var[0, 0])

    spec_var[0, 0] = 0
    var_bands = np.array([
        np.sum(spec_var[bands == k]) for k in range(0,
                                                    int(np.max(bands)) + 1)
    ])

    var_bands[0] = 0
    return (mean, var_bands)


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def create_figspace(*, fig_title=None, ion=True, **kwargs):
    # Settings:
    #    figsize                   : defaults to 16,16
    #    fig_title                 : None
    #    title_position            : 0.5, 0.94
    #    sns_style                 : seaborn style
    #    grrows, grcols (gridspec) : 4, 4
    #    parentgs                  : No parent gridspec

    if ion is True:
        plt.ion()

    # sns_style = kwargs.get('sns_style', 'darkgrid')
    # sns.set_style(sns_style)

    # Defaults
    figsize = kwargs.get('figsize', (16, 16))
    fig = plt.figure(figsize=figsize)

    # Gridspec
    grrows = kwargs.get('grrows', 4)
    grcols = kwargs.get('grcols', 4)

    parentgs = kwargs.get('parent', None)

    if parentgs is not None:
        gs = mpl.gridspec.GridSpecFromSubplotSpec(grrows,
                                                  grcols,
                                                  subplot_spec=parentgs)
    else:
        gs = mpl.gridspec.GridSpec(grrows, grcols)

    if fig_title is not None:
        tp = kwargs.get('title_position', [0.5, 0.94])
        fig.text(tp[0], tp[1], fig_title, ha='center', va='top', fontsize=14)

    return fig, plt, gs


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def start_plot(kmax, **kwargs):

    ion = True

    # --------------------------------------------------------

    mpl.rcParams['font.size'] = 14
    fig, plt, gs = create_figspace(
        ion=ion,
        # sns_style='whitegrid',
        figsize=(10, 10),
        grrows=1,
        grcols=1)
    tt = fig.text(0.5,
                  0.985,
                  kwargs.get('title', ''),
                  ha='center',
                  va='top',
                  fontsize=14)

    iplt = 0
    ax = fig.add_subplot(gs[iplt])

    # --------------------------------------------------------

    ax.grid(which='both', color='grey', ls='--', lw=0.3)

    ax.set_xscale('log', subsx=[2, 3, 4, 5, 6, 7, 8, 9])
    ax.set_yscale('log', subsy=[2, 3, 4, 5, 6, 7, 8, 9])

    ###################################
    ##set plot limits
    ###################################
    ax.set_xlim(left=5)
    ax.set_xlim(right=500)
    ax.set_ylim(bottom=1e-6)
    ax.set_ylim(top=.5)

    dom_kmax = 1022
    factork = 2 * np.pi
    lmb = 200  # Wavelength of slope switch [km]
    kx = int(dom_kmax * 3 / lmb)

    k = np.arange(1, kmax + 1)

    ax.plot(2 * k[kx:], [(k / factork)**(-5 / 3) for k in k[kx:]],
            color='grey',
            lw=0.8,
            ls='--')

    ax.plot(2 * k[:kx + 1], [(k / factork / 1.5)**(-3) for k in k[:kx + 1]],
            color='grey',
            lw=0.8,
            ls='--')

    ax2 = ax.twiny()
    ax2.set_xlim(ax.get_xlim())
    ax2.set_xscale('log')
    wl = [1000, 500, 100, 50,
          15]  # min lambda: 5dx = 15km, max lambda: 2*N*dx/(k=2) = 3066km
    kwl = [2 * dom_kmax * 3 / l for l in wl]

    ax.plot([kwl[-1], kwl[-1]], ax.get_ylim(), color='darkgrey', lw=1, ls='--')

    ax2.set_xticks(kwl)
    ax2.set_xticklabels(wl)
    ax2.grid(None)
    ax2.tick_params(axis='x', which='minor', top=False)
    ax2.tick_params(axis='x',
                    which='major',
                    direction='out',
                    width=1,
                    length=5,
                    color='lightgrey',
                    top=True)

    ax.tick_params(axis='both',
                   which='major',
                   direction='out',
                   width=1,
                   length=5,
                   color='lightgrey',
                   bottom=True,
                   left=True)
    ax.tick_params(axis='x', which='minor', bottom=False)

    ax.set_xlabel('Wavenumber k')
    ax2.set_xlabel('Wavelength [km]')
    ax.set_ylabel('KE error')

    return [ax, tt]


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------


def read_wrf_fcst(*, var, file_set, **kwargs):

    # Create a data array from given files
    # for the specified netcdf var
    # Returns:
    # A ndarray of the given variable (2D or 3D)

    # file_set can be a list or a single file
    # var is the netcdf variable to read
    # lev is an optinal keyword to specify the variable level

    if type(file_set) is not list:
        file_set = [file_set]

    lev = kwargs.get('lev', None)

    for ii, ff in enumerate(file_set):

        with Dataset(ff, 'r') as nc:
            if len(file_set) > 1:
                # np.squeeze: remove unnecessary dimensions, if any
                # np.expand_dims: to stack arrays from remaining input files
                if lev is not None:
                    mdat = np.expand_dims(np.squeeze(
                        nc.variables[var][:])[lev, ...],
                                          axis=0)
                else:
                    mdat = np.expand_dims(np.squeeze(nc.variables[var][:]),
                                          axis=0)

            else:
                # np.squeeze: remove unnecessary dimensions & return array
                if lev is not None:
                    return np.squeeze(nc.variables[var][:])[lev, ...]
                else:
                    return np.squeeze(nc.variables[var][:])

        ens = mdat if ii == 0 else np.vstack([ens, mdat])

    return ens


# --------------------------------------------------------
# --------------------------------------------------------
# --------------------------------------------------------
