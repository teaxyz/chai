select s.start_id, s.end_id 
from public.sources s 
join public.projects p 
on s.start_id = p.id 
and $1 = any(p.package_managers)
;