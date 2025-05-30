---
displayed_sidebar: docs
---

# lpad

この関数は、`str` の最初の音節から数えて `len` の長さの文字列を返します。`len` が `str` より長い場合、返される値は `str` の前にパッド文字を追加して `len` 文字に長くされます。`str` が `len` より長い場合、返される値は `len` 文字に短くされます。`len` はバイト数ではなく文字数を意味します。

## 構文

```Haskell
VARCHAR lpad(VARCHAR str, INT len[, VARCHAR pad])
```

## パラメータ

`str`: 必須、パディングされる文字列で、VARCHAR 値に評価される必要があります。

`len`: 必須、返される値の長さで、バイト数ではなく文字数を意味し、INT 値に評価される必要があります。

`pad`: 任意、`str` の前に追加される文字で、VARCHAR 値である必要があります。このパラメータが指定されていない場合、デフォルトでスペースが追加されます。

## 戻り値

VARCHAR 値を返します。

## 例

```Plain Text
MySQL > SELECT lpad("hi", 5, "xy");
+---------------------+
| lpad('hi', 5, 'xy') |
+---------------------+
| xyxhi               |
+---------------------+

MySQL > SELECT lpad("hi", 1, "xy");
+---------------------+
| lpad('hi', 1, 'xy') |
+---------------------+
| h                   |
+---------------------+

MySQL > SELECT lpad("hi", 5);
+---------------------+
| lpad('hi', 5, ' ')  |
+---------------------+
|    hi               |
+---------------------+
```

## キーワード

LPAD