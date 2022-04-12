# generic table processor

Processing static 2D data table, grouping or filtering records, and folding them into scalar data. Aimed to be very efficient and flexible, adapted to process huge data.

- Input
    - Original data files, typically csv files.
    - User defined processing rules script.
- Output
    - Processing result, which is 2D data table, presented as single csv table, csvs in directory tree or well-designed binary format.

## pipeline

- Using [PEG](https://en.wikipedia.org/wiki/Parsing_expression_grammar) parsing scripts, generating actions.
- Breaking action down into atomic parts, [topologically sorting](https://en.wikipedia.org/wiki/Topological_sorting) their dependency relations.
- Executing an action with all dependencies ready each time, caching its result until no one needs it.
- Emitting final result.

## script specification

### quick example

```
let tmp = file data.csv data1.csv;
export avg(tmp | @A == x, @B) as B_averange;
export count(tmp | @A == x) as number_count;
```

Explaination:

- `tmp` is a data collection var, init by file data.csv and data1.csv.
-  `A`, `B` is attributes, they match the origin data file's attribute name. You can refer to attributes with a `@` as a prefix.
-  `|` is `filter` operator, the left operand is data collection, and another is filter condition, it evaluates to a new collection.
-  `avg`, `count` are builtin actions, they execute a fold action to a collection, and evaluate to a fold result.
-  `export ... as ...` output a fold result to final answer, with a user defined name.

The final answer should be like this:

```
user, time, B_averange, number_count
0, 0, 23, 48
0, 1, 84, 1
1, 0, 94, 12
```

### features description

```
# data collections can be manuplated like a set
let intersection = data_a && data_b;
let union = data_a || data_b;
let difference = data_a - data_b;

# condition expression can be combined with logical operator
let result = data | @A == x && @B > y;

# fold result can be stored and used, for example, get the collections of (A > avg) and (A < avg)
# fold result binds to the data collection, operations involved different fold results and collections is ill-formed.
let avg = avg(data, @A);
let below_avg = data | @A < avg;
let above_avg = data | @A > avg;
let ill_formed = another_data | @A > avg; # ill-formed
```

### grammar specification

```
$               <- compound_stmt

compound_stmt   <- stmt
                 | stmt compound_stmt

stmt            <- decl_stmt
                 | export_stmt

expr            <- collection_expr
                 | fold_expr

collection_expr <- id
                 | file file_list
                 | collection_expr "|" cond
                 | collection_expr && collection_expr
                 | collection_expr || collection_expr
                 | collection_expr - collection_expr
                 | (collection_expr)

fold_expr       <- id
                 | builtin_action(arg_list)

arg_list        <- arg
                 | arg, arg_list

arg             <- id
                 | @id
                 | expr

decl_stmt       <- let id = expr;

export_stmt     <- export fold_expr as name;

cond (expression with a boolean value)
file_list (list of files)
id (common concept in programming language)
builtin_action (all fold actions are builtin)
name (fold result name shown in the final answer)
```

Reserved id list:

- **file** introduces a list of files to a collection
- **let** introduces vars
- **export** exports a fold result to output
- **as** introduces a name to the exported result
- **builtin_action** `avg`, `count`, etc

## core data structure

Data represent:

- **`Collection`** represents a collections of record, typically stores a set of references to the original records.
- **`FoldResult`** represents a fold result of a collection, it binds to a collection and an action pair.
- **`FoldFunc`** builtin fold action, typically closure or function object.

Script and excution engine:

- **`SymTbl`** maps symbol names to entities, it stores references to entity object.
- **`Action`** represents a certain action to do, like filtering or folding. There are dependency relations between actions.
- **`ExeEngine`** stores all actions, sorting and executing them based on their dependency relations.