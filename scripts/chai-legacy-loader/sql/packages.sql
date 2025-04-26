select 
	concat('npm', '/', project_name) as "derived_id",
	project_name as "name", 
	id as "import_id"   
from projects 
where 
	'npm' = any(package_managers)
	and created_at < '2024-01-01'::timestamp -- before ITN
	and is_spam is false -- use legacy spam filter
;