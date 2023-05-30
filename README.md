# SpiderWeibo
Collect blog posts and comments, commentator information and build a database
# 采集博主按关键字筛选博文、评论与评论者信息

该实例针对微博的反爬措施进行优化，可实现较大数据量的数据爬取需求（十万量级）实现对账号特定时间段内按关键字筛选出的所有博文的爬取，并获得博文评论与评论者个人信息。

## 数据库结构设计

爬取的数据分**博主数据**、**博文数据**、**博文评论数据（包含评论者的信息）**三种类型。在数据库中以**blogger_table**、**blog_table**、**comment_table**三个数据表分别存储。数据表间通过索引相关联（如comment_table中一条评论数据可由索引`id_blog`对应到blog_table中其所属的博文数据行），以此实现数据间的联系。

## 实例使用

您需要修改以下部分：

-文件blogger_list.csv（存放待采集博主的信息）

-main.py中key_words（关键词）

-main.py中all_post_data()的months（待采集月份）

-main.py中cookie

## 效果展示

博文数据采集数据库的展示（549,186行数据）：

![image](https://github.com/otonashi-ayana/SpiderWeibo/readme_images/屏幕截图+2023-05-30 203236.png)

其中每一行评论都会记录其所属的博文id，通过博文id可以找到博文数据表中对应的博文，实现了数据之间的关联：

