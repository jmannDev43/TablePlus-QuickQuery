# What is this

This is a TablePlus Plugin, that enables querying using an abbreviated "quick query" syntax

# Support

TablePlus build 330 and above.

# Install

### Build from source

```
git clone https://github.com/jmannDev43/TablePlus-QuickQuery.git
cd TablePlus-QuickQuery/QuickQuery.tableplusplugin
NODE_ENV=development npm install
NODE_ENV=development npm run build
open . (or double-click QuickQuery.tableplusplugin file)
```

# How to use

1. Open a SQL Query.
2. Select statement.
3. Menu: Plugins -> Quick Query (ctrl+X).
   - To speed up parsing (and re-use schema data):
   - 1. Open a SQL Query.
     2. Select Menu: Plugins -> Print Schema (ctrl+E).
     3. Cut/Paste generated JSON into `schemaData.json` (i.e. `let schemaData = { ...jsonData };`)
     4. Re-Build: `NODE_ENV=development npm run build`
     5. Re-Open: `open .` (or double-click QuickQuery.tableplusplugin file)

## Syntax
 - Query parts are separated by `|`.
 - Define a table by name or abbreviations (i.e. `my_table` abbreviation would be `mt`; other tables beginning with the same letters would become `mt1`, `mt2`, etc.)

## Syntax order

1. Table (with optional filter)
2. Joins (anything after `+`)
3. Group Bys (anything after `>`)
4. Pretty Print flag (`?`)

## Examples

- Number Filter: 
  - `orders|id|5` => `select * from orders where id = 5`
- Text Filter: 
  - `orders|customer_name|%Judy` => `select * from orders where customer_name like '%Judy%'`
- Join: 
  - `orders|id|5 + accounts` => `select * from orders o join accounts a on <generated join> where id = 5`
- Group By (with pretty print):
  - `orders|country|%United > country|state ?` => 
```
select country, state, count(1) as ct 
from orders
where id = 5
group by country, state
```

# Credits

The code to gather table schema data came from https://github.com/TablePlus/diagram-plugin.

# License

QuickQuery is released under the MIT license. See [LICENSE](https://github.com/jmannDev43/TablePlus-QuickQuery/blob/master/LICENSE) for details.
