      subroutine cal_conditions_icra
    
!!! LVerdura: actualització paràmetres de plants.plt que vulguem calibrar des del cal_parms.cal i calibration.cal

      use maximum_data_module
      use calibration_data_module
      use plant_data_module, only: pldb, id_plt
      
      implicit none
           
      character(len=16) :: chg_parm                           !                |               
      character(len=16) :: chg_typ                            !variable        |type of change (absval, abschg, pctchg)
      character(len=1) :: cond_met                            !                |       
      character(len=1) :: pl_find                             !                |
      integer :: ichg_par                                     !none            |counter
      integer :: ispu                                         !none            |counter
      integer :: ielem                                        !none            |counter
      real :: chg_val                                         !                |
      real :: absmin                                          !                |minimum range for variable
      real :: absmax                                          !                |maximum change for variable
      integer :: num_db                                       !                |
      integer :: ic                                           !none            |counter
	  real :: chg_par                                         !variable        |type of change (absval, abschg, pctchg)
         
      do ichg_par = 1, db_mx%cal_upd
        do ispu = 1, cal_upd(ichg_par)%num_elem
          ielem = cal_upd(ichg_par)%num(ispu)
          chg_parm = cal_upd(ichg_par)%name
          chg_typ = cal_upd(ichg_par)%chg_typ
          chg_val = cal_upd(ichg_par)%val
          absmin = cal_parms(cal_upd(ichg_par)%num_db)%absmin
          absmax = cal_parms(cal_upd(ichg_par)%num_db)%absmax
          num_db = cal_upd(ichg_par)%num_db
          
          !check to see if conditions are met
          cond_met = "y"
          do ic = 1, cal_upd(ichg_par)%conds
            select case (cal_upd(ichg_par)%cond(ic)%var)
            case ("plant")      !for hru
              pl_find = "n"
                if (cal_upd(ichg_par)%cond(ic)%targc == pldb(id_plt)%plantnm) then
                  pl_find = "y"
                end if
                  if (pl_find == "n") cond_met = "n"
                  exit
            end select
          end do    ! ic - conditions

          if (cond_met == "y") then
            select case (cal_upd(ichg_par)%name)
              case ("lai_pot")
                pldb(id_plt)%blai = chg_par (pldb(id_plt)%blai, ielem, chg_typ, chg_val, absmin, absmax, num_db)
              case ("bm_e")
                pldb(id_plt)%bio_e = chg_par (pldb(id_plt)%bio_e, ielem, chg_typ, chg_val, absmin, absmax, num_db)
              case ("tmp_base")
                pldb(id_plt)%t_base = chg_par (pldb(id_plt)%t_base, ielem, chg_typ, chg_val, absmin, absmax, num_db)
              end select
          end if

        end do        ! ispu
      end do          ! ichg_par
      
      return
      end subroutine cal_conditions_icra