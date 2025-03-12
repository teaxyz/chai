select 
	concat('npm/', project_name) as project_derived_key,
	project_name as name, 
	id as import_id 
from projects 
where 'npm' = any(package_managers)
;