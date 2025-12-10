-- DROP PROCEDURE edw_prod_dbo.updateglsodata();

CREATE OR REPLACE PROCEDURE edw_prod_dbo.updateglsodata()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

    CALL edw_prod_dbo.edw_log_start_job('UpdateGLSOData');

  UPDATE edw_prod_dbo.W_JOURNAL_ENTRY_F f
SET so_line_id = ar.so_line_id, 
    so_header_id = ar.so_header_id
FROM oc_prod_dbo.gl_import_references ir
JOIN oc_prod_dbo.xla_ae_lines al 
    ON ir.gl_sl_link_id = al.journalentrylineglsllinkid
JOIN oc_prod_dbo.xla_distribution_links dl 
    ON al.journalentrylineaelinenum = dl.ae_line_num
JOIN edw_prod_dbo.w_ar_invoice_sl_f ar
    ON source_system = ar.source_system
   AND al.journalentrylineaeheaderid = dl.ae_header_id
   AND dl.source_distribution_id_num_1::NUMERIC = ar.cust_trx_line_gl_dist_id
WHERE f.je_header_id = ir.je_header_id::NUMERIC
  AND f.je_line_num = ir.je_line_num::NUMERIC
  AND (COALESCE(f.so_line_id, -1) <> COALESCE(ar.so_line_id, -1) 
       OR COALESCE(f.so_header_id, -1) <> COALESCE(ar.so_header_id, -1))
  AND COALESCE(ar.so_line_id, -1) > -1
  AND ar.source_system = 'OC';


    CALL edw_prod_dbo.edw_log_update_job('UpdateGLSOData', 'C', 'Complete');
	
END;
$procedure$
;