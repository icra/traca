      module gwflow_module
    
      implicit none

      !flag
      integer :: gwflow_flag
      
      !variables for grid cells
      integer :: grid_nrow,grid_ncol,num_active
      real    :: cell_size
      real, dimension (:,:), allocatable :: head_new,head_old,gw_avail,gw_cell_satthick
      
      !flow time step
      real    :: gw_time_step
      
      !flags for writing out groundwater and nutrient balance files
      integer :: gwflag_day,gwflag_yr,gwflag_aa
      
      !boundary conditions
      integer :: bc_type
      
      !variables for linking HRUs to grid cells
      integer, dimension (:), allocatable :: num_cells_per_hru,hru_num_cells
      integer, dimension (:,:), allocatable :: cell_num_hrus
      integer, dimension (:,:,:), allocatable :: hru_cells,cell_hrus
      real, dimension (:), allocatable :: utmx_hru,utmy_hru,hru_cell_dist
      real, dimension (:,:), allocatable :: utmx_cell,utmy_cell
      integer, dimension (:,:), allocatable :: hru_cells_id
      real, dimension (:,:), allocatable :: hru_cells_fract
      real, dimension (:,:,:), allocatable :: cell_hrus_fract
      
      !variables for river cells (cells connected to SWAT+ channels)
      integer :: num_rivcells
      integer, dimension (:), allocatable :: gw_riv_id,gw_riv_row,gw_riv_col,gw_riv_chan,gw_riv_zone
      real, dimension (:), allocatable :: gw_riv_len,gw_riv_elev,gw_riv_K,gw_riv_thick

      !variables for aquifer properties and system-response
      real, dimension (:,:), allocatable :: gw_cell_top,gw_cell_bot,gw_cell_inithead,gw_cell_head
      real, dimension (:,:), allocatable :: gw_cell_K,gw_cell_Sy,gw_cell_por
      integer, dimension (:,:), allocatable :: gw_cell_id,gw_cell_status
      
      !variables for typical groundwater sources and sinks
      integer :: gw_et_flag,gw_rech_flag
      real, dimension (:), allocatable :: gwflow_perc,etremain,etactual
      real, dimension (:,:), allocatable :: gwflow_rech_sum,gwflow_et_sum,gwflow_gwsw_sum,gwflow_lateral_sum,gwflow_etact_sum,gwflow_pump_sum
      real, dimension (:,:), allocatable :: gw_cell_ss_rech,gw_cell_ss_et,gw_cell_ss_gwsw,gw_cell_ss_swgw,gw_cell_satex,gw_cell_ss_etact
      real, dimension (:,:), allocatable :: gw_cell_ss_pump,gw_cell_ss_other,gw_cell_ss,gw_cell_Q
      real, dimension (:,:), allocatable :: gw_volume_before_cell,gw_volume_after_cell
      real, dimension (:), allocatable :: chan_Q
      real, dimension (:), allocatable :: gw_delay,gw_rech
      real, dimension (:,:), allocatable :: gw_cell_exdp,gw_cell_et
      real, dimension (:,:), allocatable ::ss_rech_cell_total,ss_et_cell_total,ss_gwsw_cell_total,ss_satex_cell_total,ss_pump_cell_total,ss_etact_cell_total,ss_Q_cell_total
      
      !variables for groundwater-soil water and solute transfer
      integer :: gw_transfer_flag
      real, dimension (:,:), allocatable :: gw_cell_tran
      real, dimension (:,:), allocatable :: ss_tran_cell_total
      real, dimension (:,:), allocatable :: gwflow_tran_sum
      real, dimension (:,:), allocatable :: hru_gwtran,hru_ntran,hru_ptran
      
      !variables for tile drain flow
      integer :: gw_tile_flag,gw_tile_group_flag,gw_tile_num_group
      integer :: num_tile_cells(50)
      real    :: gw_tile_depth,gw_tile_drain_area,gw_tile_K
      integer, dimension (:,:), allocatable :: gw_cell_tile
      integer, dimension (:,:), allocatable :: gw_tilecell_rivcell
      integer, dimension (:,:,:), allocatable :: gw_tile_groups
      real, dimension (:,:), allocatable :: gwflow_tile_sum,gw_cell_ss_tile,ss_tile_cell_total

      !variables for saturation excess groundwater flow
      integer :: gw_satexcess_flag
      real, dimension (:,:), allocatable :: gw_cell_rivcell
      real, dimension (:,:), allocatable :: gwflow_satex_sum
      
      !variables for writing to files
      integer :: out_gwobs,out_gwconnect,out_gwheads,out_gwsw_chan,out_gw_chan,out_gw_rech,out_gw_et,out_gw_grid,out_gw_satex,out_gwsw,out_lateral,out_gw_etact,out_gw_tile,out_hyd_sep,out_tile_cells,out_gwobs_ss,out_gw_tran,out_gw_pump
      integer :: out_gwbal,out_gwbal_yr,out_gwbal_aa
      integer :: gw_num_output,gw_output_index
      integer, dimension (:), allocatable :: gw_output_yr,gw_output_day
      
      !observation well variables
      integer :: gw_num_obs_wells
      integer, dimension (:), allocatable :: gw_obs_cells_row,gw_obs_cells_col
      real, dimension (:), allocatable :: gw_obs_head
      
      !observation cell variables (cell with daily ss output)
      integer :: gw_cell_obs_ss_row,gw_cell_obs_ss_col
      real, dimension (:), allocatable :: gw_cell_obs_ss_vals
      
      !streamflow output
      real, dimension (:), allocatable :: channel_flow
      
      !water balance calculations
      real :: watershed_area
      real :: vol_change_yr,ss_rech_yr,ss_et_yr,ss_gw_yr,ss_sw_yr,ss_satex_yr,ss_tran_yr,ss_Q_yr,ss_pump_yr,ss_tile_yr
      real :: vol_change_total,ss_rech_total,ss_et_total,ss_gw_total,ss_sw_total,ss_satex_total,ss_tran_total,ss_Q_total,ss_pump_total,ss_tile_total
      
      !variables for hydrograph separation
      real, dimension (:,:), allocatable :: chan_hyd_sep
      integer, dimension (:), allocatable :: hydsep_flag
      
      !variables for groundwater chemical transport
      integer :: gw_transport_flag
      integer :: num_ts_transport
      integer :: out_gwconc
      real :: gw_long_disp
      real, dimension (:,:,:), allocatable :: Q_lateral
      real, dimension (:,:), allocatable :: sat_west,sat_east,sat_north,sat_south
      !no3
      integer :: out_gwbaln,out_gwbaln_yr,out_gwbaln_aa
      real :: gw_lambda_no3,gw_reta_no3
      real :: nmass_change_yr,ss_rechn_yr,ss_gwn_yr,ss_swn_yr,ss_satexn_yr,ss_advn_yr,ss_dspn_yr,ss_rctn_yr,ss_pumpn_yr,ss_tilen_yr,ss_trann_yr
      real :: nmasschange_total,ss_rechn_total,ss_gwn_total,ss_swn_total,ss_satexn_total,ss_advn_total,ss_dspn_total,ss_rctn_total,ss_pumpn_total,ss_tilen_total,ss_trann_total
      real, dimension (:,:), allocatable :: gw_cell_mn,gw_cell_cn,gw_cell_initcn,cn_new
      real, dimension (:), allocatable :: gw_obs_cn
      real, dimension (:), allocatable :: gwflow_percn,gw_rechn
      real, dimension (:,:), allocatable :: gw_nmass_before_cell,gw_nmass_after_cell
      real, dimension (:,:), allocatable :: gw_cell_ss_rechn,gw_cell_ss_gwswn,gw_cell_ss_swgwn,gw_cell_ss_pumpn,gw_cell_satexn,gw_cell_ss_tilen,nmass_adv,nmass_dsp,nmass_rct,gw_cell_ssn,gw_cell_trann,gw_cell_advn,gw_cell_dspn,gw_cell_rctn
      real, dimension (:,:), allocatable :: gwflow_rechn_sum,gwflow_gwswn_sum,gwflow_tilen_sum,gwflow_satexn_sum,gwflow_trann_sum,gwflow_pumpn_sum
      real, dimension (:,:), allocatable :: ss_rechn_cell_total,ss_gwswn_cell_total,ss_satexn_cell_total,ss_tilen_cell_total,ss_trann_cell_total,ss_pumpn_cell_total
      !p
      integer :: out_gwbalp,out_gwbalp_yr,out_gwbalp_aa
      real :: gw_reta_p
      real :: pmass_change_yr,ss_rechp_yr,ss_gwp_yr,ss_swp_yr,ss_satexp_yr,ss_advp_yr,ss_dspp_yr,ss_rctp_yr,ss_pumpp_yr,ss_tilep_yr,ss_tranp_yr
      real :: pmasschange_total,ss_rechp_total,ss_gwp_total,ss_swp_total,ss_satexp_total,ss_advp_total,ss_dspp_total,ss_rctp_total,ss_pumpp_total,ss_tilep_total,ss_tranp_total
      real, dimension (:,:), allocatable :: gw_cell_mp,gw_cell_cp,gw_cell_initcp,cp_new
      real, dimension (:), allocatable :: gw_obs_cp
      real, dimension (:), allocatable :: gwflow_percp,gw_rechp
      real, dimension (:,:), allocatable :: gw_pmass_before_cell,gw_pmass_after_cell
      real, dimension (:,:), allocatable :: gw_cell_ss_rechp,gw_cell_ss_gwswp,gw_cell_ss_swgwp,gw_cell_ss_pumpp,gw_cell_satexp,gw_cell_ss_tilep,pmass_adv,pmass_dsp,pmass_rct,gw_cell_ssp,gw_cell_tranp,gw_cell_advp,gw_cell_dspp,gw_cell_rctp
      real, dimension (:,:), allocatable :: gwflow_rechp_sum,gwflow_gwswp_sum,gwflow_tilep_sum,gwflow_satexp_sum,gwflow_tranp_sum,gwflow_pumpp_sum
      real, dimension (:,:), allocatable :: ss_rechp_cell_total,ss_gwswp_cell_total,ss_satexp_cell_total,ss_tilep_cell_total,ss_tranp_cell_total,ss_pumpp_cell_total
      !hru output
      integer :: out_gwtile_hru
      real, dimension (:,:), allocatable :: tile_hru_yr
      
      !variables for national model HRU-HUC12-cell connections
      integer, dimension (:), allocatable :: huc12_nhru,huc12_ncell,hrus_connected
      integer, dimension (:,:), allocatable :: huc12_hrus,cell_received
      integer, dimension (:,:,:), allocatable :: huc12_cells
      real(8), dimension (:), allocatable :: huc12
      
      !variables for national model applications
      logical  nat_model
      
      end module gwflow_module