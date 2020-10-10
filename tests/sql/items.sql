-- name: get-list
  select title,
         revealed
    from items
   where flag = :flag
order by revealed desc;

-- name: get_list_alt
  select title,
         revealed
    from items
   where flag = :flag
order by revealed desc;

-- name: create_item>
insert into items (
  title,
  revealed,
  flag
) values (
  :title,
  :revealed,
  :flag
);