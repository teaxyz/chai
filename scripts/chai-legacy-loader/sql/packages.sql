select 
    npr.project_derived_key as derived_key,
    npr.project_name as "name",
    npr.project_id as import_id    
from chai_staging.npm_projects_raw npr
;