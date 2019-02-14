drop table if exists movie_comments;
create table movie_comments
(
  id           int          not null primary key,
  subject_id   int          null,
  content      text         null,
  author_uid   varchar(255) null,
  rating       decimal      null,
  useful_count int          null,
  created_at   timestamp    null,
  updated_at   timestamp    null
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

drop table if exists movie_reviews;
create table movie_reviews
(
  id           int          not null primary key,
  alt          varchar(255) null,
  subject_id   int          null,
  title        mediumtext   null,
  summary      mediumtext   null,
  share_url    mediumtext   null,
  content      text         null,
  author_uid   varchar(255) null,
  rating       decimal      null,
  useful_count int          null,
  useless_count int         null,
  comments_count int        null,
  created_at   timestamp    null,
  updated_at   timestamp    null
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
