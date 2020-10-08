-- name: get-list-3
  select title,
         revealed
    from items
   where flag = :flag
order by revealed desc;

-- name: get_list_4
  select title,
         revealed
    from items
   where flag = :flag
order by revealed desc;