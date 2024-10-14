# Chai Data Exploration

```sql
-- Packages with the longest lifetime
SELECT p.name,
SUM(v.downloads) AS "downloads",
count(v.package_id) AS versions,
min(v.published_at) AS "first published",
max(v.published_at) AS "last published",
max(v.published_at) - min(v.published_at) AS lifetime
FROM packages AS p
JOIN versions v ON v.package_id = p.id
GROUP BY p.name
ORDER BY lifetime DESC limit 100;

-- Packages sorted by dependents
SELECT p.name, count(d.id) AS dependents
FROM packages AS p
JOIN dependencies AS d ON d.dependency_id = p.id
GROUP BY p.name
ORDER BY count(d.id) DESC LIMIT 100;

-- Packages sorted by dependents with lifetime
SELECT p.name,
count(d.id) AS dependents,
min(v.published_at) AS "first published",
max(v.published_at) AS "last published",
max(v.published_at) - min(v.published_at) AS lifetime
FROM packages AS p
JOIN dependencies AS d ON d.dependency_id = p.id
JOIN versions v ON v.package_id = p.id
GROUP BY p.name
ORDER BY count(d.id) DESC LIMIT 100;

-- Packages sorted by dependents with downloads
SELECT p.name,
count(d.id) AS dependents,
sum(v.downloads) AS downloads
FROM packages AS p
JOIN dependencies AS d ON d.dependency_id = p.id
JOIN versions v ON v.package_id = p.id
GROUP BY p.name
ORDER BY count(d.id) DESC LIMIT 100;

-- Packages with most dependents sorted by download/dependent ratio
SELECT name, dependents, downloads, (downloads / dependents) AS ratio FROM
(SELECT p.name,
count(d.id) AS dependents,
sum(v.downloads) AS downloads
FROM packages AS p
JOIN dependencies AS d ON d.dependency_id = p.id
JOIN versions v ON v.package_id = p.id
GROUP BY p.name
ORDER BY count(d.id) DESC LIMIT 1000)
ORDER BY ratio DESC;
```
