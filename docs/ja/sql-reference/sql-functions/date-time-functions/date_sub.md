---
displayed_sidebar: docs
---

# date_sub,subdate

指定された時間間隔を日付から減算します。

## Syntax

```Haskell
DATETIME DATE_SUB(DATETIME|DATE date,INTERVAL expr type)
```

## Parameters

- `date`: 有効な DATE または DATETIME 式でなければなりません。
- `expr`: 減算したい時間間隔です。INT 型でなければなりません。
- `type`: 時間間隔の単位です。以下のいずれかの値にのみ設定できます: YEAR, QUARTER, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND (3.1.7 以降), MICROSECOND (3.1.7 以降)。

## Return value

DATETIME 値を返します。日付が存在しない場合、例えば `2020-02-30`、または日付が DATE または DATETIME 値でない場合、NULL が返されます。

## Examples

```Plain Text
select date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY);
+-------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 DAY) |
+-------------------------------------------------+
| 2010-11-28 23:59:59                             |
+-------------------------------------------------+

select date_sub('2010-11-30', INTERVAL 2 hour);
+-----------------------------------------+
| date_sub('2010-11-30', INTERVAL 2 HOUR) |
+-----------------------------------------+
| 2010-11-29 22:00:00                     |
+-----------------------------------------+

select date_sub('2010-02-30', INTERVAL 2 DAY);
+----------------------------------------+
| date_sub('2010-02-30', INTERVAL 2 DAY) |
+----------------------------------------+
| NULL                                   |
+----------------------------------------+

select date_sub('2010-11-30 23:59:59', INTERVAL 2 QUARTER);
+-----------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 QUARTER) |
+-----------------------------------------------------+
| 2010-05-30 23:59:59                                 |
+-----------------------------------------------------+

select subdate('2010-11-30 23:59:59', INTERVAL 2 millisecond);
+--------------------------------------------------------+
| subdate('2010-11-30 23:59:59', INTERVAL 2 MILLISECOND) |
+--------------------------------------------------------+
| 2010-11-30 23:59:58.998000                             |
+--------------------------------------------------------+

select date_sub('2010-11-30 23:59:59', INTERVAL 2 microsecond);
+---------------------------------------------------------+
| date_sub('2010-11-30 23:59:59', INTERVAL 2 MICROSECOND) |
+---------------------------------------------------------+
| 2010-11-30 23:59:58.999998                              |
+---------------------------------------------------------+
```