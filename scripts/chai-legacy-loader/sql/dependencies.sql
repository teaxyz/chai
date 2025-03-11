select 
    nhvr.start_id as package_import_id,
    ndr.end_id as dependency_import_id
from chai_staging.npm_depends_on_raw ndr 
join chai_staging.npm_has_versions_raw nhvr 
on ndr.start_id = nhvr.end_id 
;