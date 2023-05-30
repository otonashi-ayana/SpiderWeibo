# SpiderWeibo
Collect blog posts and comments, commentator information and build a database
# 采集博主按关键字筛选博文、评论与评论者信息

该实例针对微博的反爬措施进行优化，可实现较大数据量的数据爬取需求（十万量级）实现对账号特定时间段内按关键字筛选出的所有博文的爬取，并获得博文评论与评论者个人信息。

## 数据库结构设计

爬取的数据分**博主数据**、**博文数据**、**博文评论数据（包含评论者的信息）**三种类型。在数据库中以**blogger_table**、**blog_table**、**comment_table**三个数据表分别存储。数据表间通过索引相关联（如comment_table中一条评论数据可由索引`id_blog`对应到blog_table中其所属的博文数据行），以此实现数据间的联系。

建库sql语句

```
DROP TABLE IF EXISTS blogger_base,blog_base,comment_base;
create table blogger_base
(
    id_blogger_base int(11) NOT NULL AUTO_INCREMENT, #自增类型
    blogger_name varchar(50) NOT NULL DEFAULT 'null' COMMENT '博主名称',
    blogger_id varchar(20) NOT NULL DEFAULT 'null' COMMENT '博主id',
    blogger_region varchar(50) NOT NULL DEFAULT 'null' COMMENT '博主地域来源',
    blogger_fans int(11) NOT NULL DEFAULT -100 COMMENT '博主粉丝',
    blogger_follow int(11) NOT NULL DEFAULT -100 COMMENT '博主关注',
    blogger_gender char(1) NOT NULL DEFAULT 'n' COMMENT '博主性别',
    blogger_weibo int NOT NULL DEFAULT -100 COMMENT '博主微博数量',
    blogger_veri_type int NOT NULL DEFAULT -100 COMMENT '博主认证类型',
    blogger_veri_info varchar(255) NOT NULL DEFAULT 'null' COMMENT '博主认证信息',
    PRIMARY KEY (id_blogger_base)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

create table blog_base
(
    id_blog_base int(11) NOT NULL AUTO_INCREMENT,
    blog_date char(30) NOT NULL DEFAULT 'null' COMMENT '博文发布时间',
    blog_text varchar(6000) NOT NULL DEFAULT 'null' COMMENT '博文内容',
    blog_id varchar(20) NOT NULL DEFAULT 'null' COMMENT '博文id',
    blog_comments int NOT NULL DEFAULT -100 COMMENT '博文评论量',
    blog_likes int NOT NULL DEFAULT -100 COMMENT '博文点赞',
    blog_keywords varchar(100) NOT NULL DEFAULT 'null' COMMENT '博文关键词',
    retweeted_blogger_name varchar(50) NOT NULL DEFAULT 'null' COMMENT '转推原博主名称',
    retweeted_verified_type int NOT NULL DEFAULT -100 COMMENT '转推原博主认证',
    retweeted_text varchar(3000) NOT NULL DEFAULT 'null' COMMENT '转推原文',
    id_blogger varchar(20) NOT NULL DEFAULT 'null' COMMENT '所属博主id',
    PRIMARY KEY (id_blog_base)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

create table comment_base
(
    id_comment_base int(11) NOT NULL AUTO_INCREMENT,
    comment_date char(30) NOT NULL DEFAULT 'null' COMMENT '评论发布时间',
    comment_text varchar(6000) NOT NULL DEFAULT 'null' COMMENT '评论内容',
    commenter_name varchar(50) NOT NULL DEFAULT 'null' COMMENT '评论者名称',
    commenter_region varchar(50) NOT NULL DEFAULT 'null' COMMENT '评论地域来源',
    commenter_id varchar(20) NOT NULL DEFAULT 'null' COMMENT '评论者id',
    commenter_fans int(11) NOT NULL DEFAULT -100 COMMENT '评论者粉丝',
    commenter_follow int(11) NOT NULL DEFAULT -100 COMMENT '评论者关注',
    commenter_gender char(1) NOT NULL DEFAULT 'n' COMMENT '评论者性别',
    commenter_weibo int NOT NULL DEFAULT -100 COMMENT '评论者微博数量',
    commenter_veri_type int NOT NULL DEFAULT -100 COMMENT '评论者认证类型',
    commenter_veri_info varchar(255) NOT NULL DEFAULT 'null' COMMENT '评论者认证信息',
    id_blog varchar(20) NOT NULL DEFAULT 'null' COMMENT '所属博文id',
    PRIMARY KEY (id_comment_base)
)ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

```



## 实例使用

您需要修改以下部分：

-文件blogger_list.csv（存放待采集博主的信息）

-main.py中key_words（关键词）

-main.py中所连接数据库的相关信息

-main.py中all_post_data()的months（待采集月份）

-main.py中cookie

## 效果展示

博文数据采集数据库的展示（549,186行数据）：

![](https://github.com/otonashi-ayana/SpiderWeibo/blob/main/readme_images/screenshots.png)

其中每一行评论都会记录其所属的博文id，通过博文id可以找到博文数据表中对应的博文，实现了数据之间的关联：

![](https://github.com/otonashi-ayana/SpiderWeibo/blob/main/readme_images/screenshots2.png)


本人代码水平堪忧，本代码中的多线程部分与异常处理非常混乱（），同时该项目前后经过了很长一段时间修改，其屎山程度惨不忍睹。

不过本实例中的一些细节问题在网络上没有查阅到相关资料，希望能够帮助到遇到同样问题的各位。
