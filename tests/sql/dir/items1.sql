-- name: get-list-1
  select title,
         revealed
    from items
   where flag = :flag
order by revealed desc;

-- name: get_list_2
  select title,
         revealed
    from items
   where flag = :flag
order by revealed desc;