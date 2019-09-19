include Map.Make({
  type t = string;
  let compare = (a, b) =>
    String.(compare(lowercase_ascii(a), lowercase_ascii(b)));
});

let change = (~key, ~f, map) =>
  switch (f(find_opt(key, map))) {
  | None => remove(key, map)
  | Some(v) => add(key, v, map)
  };

let add = (~key, ~value, map) => add(key, value, map);

let add_opt = (~key, ~value, map) =>
  switch (value) {
  | Some(value) => add(~key, ~value, map)
  | None => map
  };
