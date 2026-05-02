-- Databricks notebook source


INSERT INTO myproject_dev.etlconfig.source
SELECT uuid(), 'AdventureWorks', 'awk', 'SQL Server Database Storing Information About Finance Data', current_timestamp(), current_timestamp(), current_user(), current_user()

