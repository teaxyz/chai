# Manual Data Tasks Post Canon Name Migration

Documenting all the tasks that need to be completed once the canon name migration was
completed

## Completed Tasks

- [x] Fix all URLs that are in the URLs table

## In Progress Tasks

- [x] Update the Canon IDs for registered projects to be the old canon ID

## Future Tasks

- [ ] Remove all Bad URLs from the URLs table
- [ ] Investigate pending non canon URLs in URL

## Implementation Plan

### Update the Canon IDs for registered projects

- You are given a list of canon IDs, in the form of a set of UUIDs passed to stdin
- For each of those canon_ids:
  - Join to `canon_packages_old` to obtain the `package_id`
  - Join to `canon_packages` to obtain the current `canon_id`
  - Run the following 3 update statements to update the package
    ```sql
    UPDATE canons
    SET id = :old_canon_id
    WHERE id = :new_canon_id
    ;
    UPDATE canon_packages
    SET canon_id = :old_canon_id
    WHERE canon_id = :new_canon_id
    ;
    UPDATE tea_ranks
    SET canon_id = :old_canon_id
    WHERE canon_id = :new_canon_id
    ```
- In the case where the join to `canon_packages_old` does not yield a package_id, issue
  a warning statement to stdout with the canon_id
- In the case where the join to `canon_packages` does not yield a package_id, issue a
  warning statement to stdout with the canon_id
- At program execution, publish the following:
  - Stdout:
    ```
    --------------------------------------------------
    ✅ Success: <COUNT OF UPDATED CANONS>
    ❌ Failure: <COUNT OF NOT UPDATED CANONS>
    --------------------------------------------------
    ```
  - File:
    ```csv
    canon_id,reason
    00000000-0000-0000-0000-000000000001,could not find package_id
    00000000-0000-0000-0000-000000000002,could not find new canon_id
    ```
    where the reasons correspond to which join failed
- Include a `--dry-run` mode where the code simply tells you what it is going to do,
  without connecting to the db or running any queries
- Use `@db.py` in the `scripts/upgrade_canons` directory to manage database interactions

### Relevant Files

- path/to/file1.ts - Description of purpose
- path/to/file2.ts - Description of purpose
