---
displayed_sidebar: docs
---

# days_sub

指定された日数を日付または日時から引いて、新しい日時を取得します。

## 構文

```Haskell
DATETIME days_sub(DATETIME|DATE d, INT n);
```

## パラメータ

`d`: 日付または日時の式。

`n`: `d` から引く日数。

## 戻り値

日時型の値を返します。いずれかのパラメータが NULL または無効な場合、NULL が返されます。

結果が範囲 [0000-01-01 00:00:00, 9999-12-31 00:00:00] を超える場合、NULL が返されます。

## 例

```Plain Text
SELECT DAYS_SUB('2022-12-20 12:00:00', 10);
+--------------------------------------------------+
| days_sub('2022-12-20 12:00:00', INTERVAL 10 DAY) |
+--------------------------------------------------+
| 2022-12-10 12:00:00                              |
+--------------------------------------------------+

SELECT DAYS_SUB('2022-12-20 12:00:00', -10);
+---------------------------------------------------+
| days_sub('2022-12-20 12:00:00', INTERVAL -10 DAY) |
+---------------------------------------------------+
| 2022-12-30 12:00:00                               |
+---------------------------------------------------+

SELECT DAYS_SUB('2022-12-20 12:00:00', 738874);
+------------------------------------------------------+
| days_sub('2022-12-20 12:00:00', INTERVAL 738874 DAY) |
+------------------------------------------------------+
| 0000-01-01 12:00:00                                  |
+------------------------------------------------------+

SELECT DAYS_SUB('2022-12-20 12:00:00', 738875);
+------------------------------------------------------+
| days_sub('2022-12-20 12:00:00', INTERVAL 738875 DAY) |
+------------------------------------------------------+
| NULL                                                 |
+------------------------------------------------------+

SELECT DAYS_SUB('2022-12-20 12:00:00', -2913550);
+--------------------------------------------------------+
| days_sub('2022-12-20 12:00:00', INTERVAL -2913550 DAY) |
+--------------------------------------------------------+
| 9999-12-31 12:00:00                                    |
+--------------------------------------------------------+

SELECT DAYS_SUB('2022-12-20 12:00:00', -2913551);
+--------------------------------------------------------+
| days_sub('2022-12-20 12:00:00', INTERVAL -2913551 DAY) |
+--------------------------------------------------------+
| NULL                                                   |
+--------------------------------------------------------+
```

## キーワード

DAY, day