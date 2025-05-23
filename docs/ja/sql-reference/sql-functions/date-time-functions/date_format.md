---
displayed_sidebar: docs
---

# date_format

指定されたフォーマットに従って日付を文字列に変換します。現在、最大128バイトの文字列をサポートしています。返される値の長さが128を超える場合、NULLが返されます。

## 構文

```Haskell
VARCHAR DATE_FORMAT(DATETIME date, VARCHAR format)
```

## パラメータ

- `date` パラメータは、有効な日付または日付式である必要があります。

- `format` は、日付または時刻の出力フォーマットを指定します。

利用可能なフォーマットは以下の通りです：

```Plain Text
%a | 曜日の省略名 (Sun から Sat)
%b | 月の省略名 (Jan から Dec)
%c | 月の数値表現 (0-12)
%D | 日を数値で表し、英語の接尾辞を付ける
%d | 日を数値で表す (00-31)
%e | 日を数値で表す (0-31)
%f | マイクロ秒
%H | 時 (00-23)
%h | 時 (01-12)
%I | 時 (01-12)
%i | 分 (00-59)
%j | 年の日数 (001-366)
%k | 時 (0-23)
%l | 時 (1-12)
%M | 月のフルネーム
%m | 月を数値で表す (00-12)
%p | AM または PM
%r | 12時間制の時刻 (hh:mm:ss AM または PM)
%S | 秒 (00-59)
%s | 秒 (00-59)
%T | 24時間制の時刻 (hh:mm:ss)
%U | 週 (00-53) で、日曜日が週の最初の日
%u | 週 (00-53) で、月曜日が週の最初の日
%V | 週 (01-53) で、日曜日が週の最初の日。%X と共に使用。
%v | 週 (01-53) で、月曜日が週の最初の日。%x と共に使用。
%W | 曜日のフルネーム
%w | 週の日で、日曜日=0、土曜日=6
%X | 週の年で、日曜日が週の最初の日。4桁の値。%V と共に使用。
%x | 週の年で、月曜日が週の最初の日。4桁の値。%v と共に使用。
%Y | 年。4桁の値。
%y | 年。2桁の値。
%% | % を表す。
```

## 例

```Plain Text
MySQL > select date_format('2009-10-04 22:23:00', '%W %M %Y');
+------------------------------------------------+
| date_format('2009-10-04 22:23:00', '%W %M %Y') |
+------------------------------------------------+
| Sunday October 2009                            |
+------------------------------------------------+

MySQL > select date_format('2007-10-04 22:23:00', '%H:%i:%s');
+------------------------------------------------+
| date_format('2007-10-04 22:23:00', '%H:%i:%s') |
+------------------------------------------------+
| 22:23:00                                       |
+------------------------------------------------+

MySQL > select date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
+------------------------------------------------------------+
| date_format('1900-10-04 22:23:00', '%D %y %a %d %m %b %j') |
+------------------------------------------------------------+
| 4th 00 Thu 04 10 Oct 277                                   |
+------------------------------------------------------------+

MySQL > select date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
+------------------------------------------------------------+
| date_format('1997-10-04 22:23:00', '%H %k %I %r %T %S %w') |
+------------------------------------------------------------+
| 22 22 10 10:23:00 PM 22:23:00 00 6                         |
+------------------------------------------------------------+

MySQL > select date_format('1999-01-01 00:00:00', '%X %V');
+---------------------------------------------+
| date_format('1999-01-01 00:00:00', '%X %V') |
+---------------------------------------------+
| 1998 52                                     |
+---------------------------------------------+

MySQL > select date_format('2006-06-01', '%d');
+------------------------------------------+
| date_format('2006-06-01 00:00:00', '%d') |
+------------------------------------------+
| 01                                       |
+------------------------------------------+

MySQL > select date_format('2006-06-01', '%%%d');
+--------------------------------------------+
| date_format('2006-06-01 00:00:00', '%%%d') |
+--------------------------------------------+
| %01                                        |
+--------------------------------------------+
```

## キーワード

DATE_FORMAT,DATE,FORMAT