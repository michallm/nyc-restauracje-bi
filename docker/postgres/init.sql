create database nyc;
create database prefect;
create database dagster;
create database superset;
\connect nyc
create extension if not exists postgis;
create schema if not exists staging;
