select 
	id as import_id,
	"source", 
	homepage 
from projects
where $1 = any(package_managers)