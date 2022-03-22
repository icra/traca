     subroutine header_rel

!! LVerdura: reservoir release output file
    
     use basin_module
     use reservoir_module
     use hydrograph_module
     
     implicit none 
      if (pco%res%d == "y" .and. sp_ob%res > 0 ) then
        open (2614,file="reservoir_release.txt",recl=1500)
        write (2614,*) bsn%name, prog
        write (2614,*) rel_hdr
        write (2614,*) rel_units
        write (9000,*) "RES                       reservoir_release.txt"
      end if

      return
      end subroutine header_rel  