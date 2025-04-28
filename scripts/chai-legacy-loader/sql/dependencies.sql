-- from old CHAI's structure, the sources table stores dependencies from package to 
-- package
-- the projects tables stores the package managers themselves, which is where we apply
-- the where clause
select s.start_id, s.end_id 
from public.sources s 
join public.projects p 
on s.start_id = p.id 
and $1 = any(p.package_managers)
;