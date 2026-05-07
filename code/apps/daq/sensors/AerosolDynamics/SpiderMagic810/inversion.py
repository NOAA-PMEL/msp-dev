import numpy as np
from scipy import interpolate

# ==========================================
# Module-Level Physics Helper Functions
# ==========================================

def mu(T):
    """Gas dynamic viscosity [kg/m-s]"""
    return 1.83e-5 * (296.15 + 110.4) / (T + 110.4) * (T / 296.15)**1.5
    
def lambda_gas(T, P):
    """Gas mean free path [m]"""
    return 6.73e-8 * (101.3 / P) * (T / 296.15) * (1 + 110.4 / 296.15) / (1 + 110.4 / T)
    
def Cc_func(Dp_nm, T, P):
    """Cunningham Slip Correction Factor"""
    Dp_m = Dp_nm * 1e-9
    Kn = 2 * lambda_gas(T, P) / Dp_m
    return 1 + Kn * (1.142 + 0.558 * np.exp(-0.999 / Kn))

def Zp_to_Dp(Zp_arr, n_ch, T, P):
    """Convert Electrical Mobility to Particle Diameter [nm]"""
    e_c = 1.60218e-19
    Zp_arr = np.array(Zp_arr, dtype=float)
    
    # Approximate initial guess
    Cc_guess = 15990 * Zp_arr**0.5028 + 0.4203
    Dp_guess = n_ch * e_c * Cc_guess / (3 * np.pi * mu(T) * Zp_arr)
    
    # Iterative solver
    Dp_m = Dp_guess.copy()
    for i in range(len(Zp_arr)):
        ite = 0
        while ite < 500:
            Dp_f = n_ch * e_c * Cc_func(Dp_m[i] * 1e9, T, P) / (3 * np.pi * mu(T) * Zp_arr[i])
            if np.abs(Dp_m[i] - Dp_f) / Dp_m[i] < 1e-9:
                Dp_m[i] = Dp_f
                break
            Dp_m[i] = Dp_m[i] + (Dp_f - Dp_m[i]) / 2.0
            ite += 1
    return Dp_m * 1e9 

def Dp_to_Zp(Dp_nm, n_ch, T, P):
    """Convert Particle Diameter to Electrical Mobility [m^2/V-s]"""
    e_c = 1.60218e-19
    Dp_m = Dp_nm * 1e-9
    return n_ch * e_c * Cc_func(Dp_nm, T, P) / (3 * np.pi * mu(T) * Dp_m)

def f_Wdnslr(Dp_nm, n_ch, T):
    """Wiedensohler bipolar charge probability"""
    Dp_nm = np.array([Dp_nm]).flatten()
    alpha = np.array([[-26.3328, -2.3197, -0.0003, -2.3484, -44.4756],
                      [ 35.9044,  0.6175, -0.1014,  0.6044,  79.3772],
                      [-21.4608,  0.6201,  0.3073,  0.4800, -62.8900],
                      [ 07.0867, -0.1105, -0.3372,  0.0013,  26.4492],
                      [-01.3088, -0.1260,  0.1023, -0.1553, -05.7480],   
                      [ 00.1051,  0.0297, -0.0105,  0.0320,  00.5049]])
    
    if -2 <= n_ch <= 2:
        f_ch = 10**np.sum(np.log10(Dp_nm).reshape(-1,1)**np.arange(6) * alpha[:, n_ch+2], axis=1)
    else:
        e_c, e_0, k_B, Z_ratio = 1.60217733e-19, 8.854187817e-12, 1.380658e-23, 0.875
        F = 2 * np.pi * e_0 * (Dp_nm*1e-9) * k_B * T / e_c**2
        f_ch = e_c / (4 * np.pi**2 * e_0 * (Dp_nm*1e-9) * k_B * T)**0.5 * \
               np.exp( -(n_ch - F * np.log(Z_ratio))**2 / (2 * F) )
    return f_ch if len(Dp_nm) > 1 else f_ch[0]

def calc_diffusion_loss(Dp_nm, L_tube, Q_lpm, d_tube_mm, T, P):
    """Gormley-Kennedy diffusional loss calculation"""
    Dp_m = np.array(Dp_nm) * 1e-9
    Q_m3s = (Q_lpm / 60.0) * 1e-3
    D_diff = (1.380649e-23 * T * Cc_func(Dp_nm, T, P)) / (3 * np.pi * mu(T) * Dp_m)
    mu_dim = (np.pi * D_diff * L_tube) / Q_m3s
    eta = np.zeros_like(mu_dim)
    mask = mu_dim < 0.009
    eta[mask] = 1 - 2.56*mu_dim[mask]**(2/3) + 1.2*mu_dim[mask] + 0.177*mu_dim[mask]**(4/3)
    eta[~mask] = 0.819*np.exp(-3.657*mu_dim[~mask]) + 0.097*np.exp(-22.3*mu_dim[~mask]) + 0.032*np.exp(-57*mu_dim[~mask])
    return np.clip(eta, 0.0, 1.0)


# ==========================================
# Inversion Strategy Classes
# ==========================================

class SpiderInversionBase:
    """Base class for all Spider-MAGIC inversions."""
    def __init__(self, config=None):
        self.config = config or {}
        # Hardware geometric defaults
        self.L_tube_m = self.config.get("L_tube_m", 1.5)
        self.d_tube_mm = self.config.get("d_tube_mm", 4.5)
        self.num_diameter_bins = self.config.get("num_diameter_bins", 60)
        
        # Spider DMA physical constants
        self.b_gap = 0.005           # electrode gap [m]
        self.r2_out = 0.045          # outer radius [m]
        self.r1_in = 0.0024          # inner radius [m]
        self.G_geom = self.b_gap / (np.pi * (self.r2_out**2 - self.r1_in**2))

    def _get_bin_idx(self, val, arr):
        """Helper to find float index for interpolation."""
        if val < arr[0] or val > arr[-1]: 
            return None
        return interpolate.interp1d(arr, np.arange(len(arr)), kind='linear')(val)

    def invert(self, record):
        raise NotImplementedError("Subclasses must implement the invert method.")


class StandardInversion(SpiderInversionBase):
    """
    Standard physics-based inversion using sequential stripping and Gormley-Kennedy 
    diffusion loss corrections.
    """
    def __init__(self, config=None):
        super().__init__(config)
        self.max_charges = self.config.get("max_charges", 3)

    def invert(self, record):
        try:
            # 1. Environmental Conditions
            T_C = np.nanmean(record["variables"]["input_T"]["data"]) 
            P_mbar = np.nanmean(record["variables"]["abs_pressure"]["data"])
            T = (T_C if not np.isnan(T_C) else 20.0) + 273.15
            P = (P_mbar if not np.isnan(P_mbar) else 1013.25) / 10.0
            
            # Flows
            Qsh_ccm = np.nanmean(record["variables"]["sh_flow"]["data"]) or 900.0
            Qa_ccm = np.nanmean(record["variables"]["aer_flow"]["data"]) or 300.0
            beta = Qa_ccm / Qsh_ccm
            
            # 2. Extract and isolate the active scan ramp
            V_raw = np.array(record["variables"]["read_V"]["data"], dtype=float)
            C_raw = np.array(record["variables"]["concentration"]["data"], dtype=float)
            V_abs = np.abs(V_raw)
            
            # Find monotonic active ramp (ignores steady state holds at start/end)
            diff_V = np.abs(np.diff(V_abs))
            moving = diff_V > 0.5
            
            if not np.any(moving):
                raise ValueError("No active voltage ramp found in scan.")
                
            start_idx = np.argmax(moving)
            end_idx = len(moving) - np.argmax(moving[::-1]) 
            
            V_ramp = V_abs[start_idx:end_idx+1]
            C_ramp = C_raw[start_idx:end_idx+1]
            
            # Sort ascending for interpolation
            if V_ramp[0] > V_ramp[-1]:
                V_ramp = V_ramp[::-1]
                C_ramp = C_ramp[::-1]

            V_ramp = np.clip(V_ramp, 1.0, 6000.0)

            # 3. Phase 1 Inversion (Apparent dN/dlogDp)
            Zp_ramp = self.G_geom * ((Qsh_ccm / 60.0) * 1e-6) / V_ramp
            Dp_ramp = Zp_to_Dp(Zp_ramp, n_ch=1, T=T, P=P)
            
            f_n1 = f_Wdnslr(Dp_ramp, n_ch=1, T=T)
            eta_n1 = calc_diffusion_loss(Dp_ramp, self.L_tube_m, Qa_ccm/1000.0, self.d_tube_mm, T, P)
            
            L_sngl = (beta * f_n1 * eta_n1) / 1.0  # Assumes a_star = 1.0
            
            dNdlogDp_app = np.zeros_like(C_ramp)
            valid = L_sngl > 1e-6
            dNdlogDp_app[valid] = C_ramp[valid] / L_sngl[valid] / np.log10(np.e)

            # 4. Phase 2 Inversion (Sequential Stripping)
            dNdlogDp_strip = np.copy(dNdlogDp_app)
            
            for i in range(len(Dp_ramp) - 1, -1, -1):
                if dNdlogDp_strip[i] <= 0 or f_n1[i] < 1e-6: 
                    continue
                    
                for n in range(2, self.max_charges + 1):
                    p_nn = f_Wdnslr(Dp_ramp[i], n_ch=n, T=T)
                    if p_nn == 0: 
                        break
                    
                    Z_leak = Dp_to_Zp(Dp_ramp[i], n, T, P)
                    D_app = Zp_to_Dp([Z_leak], n_ch=1, T=T, P=P)[0]
                    e_app = calc_diffusion_loss([D_app], self.L_tube_m, Qa_ccm/1000.0, self.d_tube_mm, T, P)[0]
                    
                    N_leak = dNdlogDp_strip[i] * (p_nn / f_n1[i]) * (eta_n1[i] / e_app)
                    
                    idx_f = self._get_bin_idx(D_app, Dp_ramp)
                    if idx_f is not None:
                        idx_l, idx_h = int(np.floor(idx_f)), int(np.ceil(idx_f))
                        w_h = idx_f - idx_l
                        if idx_l >= 0: 
                            dNdlogDp_strip[idx_l] -= N_leak * (1.0 - w_h)
                        if idx_h < len(Dp_ramp) and idx_h != idx_l: 
                            dNdlogDp_strip[idx_h] -= N_leak * w_h

            dNdlogDp_strip = np.clip(dNdlogDp_strip, 0, None)

            # 5. Resample to Fixed 60-Bin Output
            std_Dp = np.logspace(np.log10(10), np.log10(500), self.num_diameter_bins)
            
            f_interp = interpolate.interp1d(Dp_ramp, dNdlogDp_strip, bounds_error=False, fill_value=0.0)
            std_dNdlogDp = np.clip(f_interp(std_Dp), 0.0, None)
            
            std_dlogDp = np.gradient(np.log10(std_Dp))
            std_dN = std_dNdlogDp * std_dlogDp
            intN = np.nansum(std_dN)

            # Update Record
            record["variables"]["diameter"]["data"] = np.round(std_Dp, 3).tolist()
            record["variables"]["dN"]["data"] = np.round(std_dN, 3).tolist()
            record["variables"]["dlogDp"]["data"] = np.round(std_dlogDp, 5).tolist()
            record["variables"]["dNdlogDp"]["data"] = np.round(std_dNdlogDp, 3).tolist()
            record["variables"]["intN"]["data"] = float(np.round(intN, 3))

        except Exception as e:
            # Fallback to None if inversion fails (avoids shape mismatch)
            record["variables"]["diameter"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["dN"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["dlogDp"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["dNdlogDp"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["intN"]["data"] = None

        return record


class AerosolDynamicsInversion(SpiderInversionBase):
    """
    Proprietary inversion routine provided by Aerosol Dynamics.
    """
    def __init__(self, config=None):
        super().__init__(config)
        # TODO: Add any specific setup (e.g., loading transfer matrices) here

    def invert(self, record):
        # TODO: Implement Aerosol Dynamics specific logic here
        
        # Example safety block
        try:
            pass
        except Exception as e:
            record["variables"]["diameter"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["dN"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["dlogDp"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["dNdlogDp"]["data"] = [None] * self.num_diameter_bins
            record["variables"]["intN"]["data"] = None

        return record