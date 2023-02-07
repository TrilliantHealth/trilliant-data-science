"""The purpose of this module is to allow overlaying a base config
with StackContext, such that configuration can be overridden on a
per-thread basis within a running application.

Essentially this is an extension of the idea of dependency injection
to the concept of config.
"""
