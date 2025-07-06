select 
	id as import_id,
	"source", 
	homepage 
from projects
where 
	'npm' = any(package_managers)
	and created_at < '2024-01-01'::timestamp -- before ITN
	and is_spam is false -- use legacy spam filter