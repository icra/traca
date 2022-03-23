     subroutine header_tplant

!! LVerdura: plant transpiration and water stress
    
     use basin_module
     
     implicit none 
      if (pco%wb_hru%d == "y") then
        open (2618,file="plant_transpiration.txt",recl=1500)
        write (2618,*) bsn%name, prog
        write (2618,*) tpl_hdr
        write (2618,*) tpl_uts
        write (9000,*) "HRU                      plant_transpiration.txt"
      end if

      return
      end subroutine header_tplant  