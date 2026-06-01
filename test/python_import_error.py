# Databricks notebook source
# Real-world pattern: a job deploys with a missing package or wheel dependency.
import vendor_specific_transform_library_that_is_not_installed  # noqa: F401

print("This line should never execute.")
