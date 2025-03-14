select 
	concat($1, '/', project_name) as project_derived_key,
	project_name as name, 
	id as import_id 
from projects 
where $1 = any(package_managers)
;