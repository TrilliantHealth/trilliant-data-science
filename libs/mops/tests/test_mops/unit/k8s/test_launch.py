from thds.mops.k8s.launch import construct_job_name


def test_job_name_len():
    long_prefix = "a very long Prefix that has Bad characters and Stuff"
    assert len(long_prefix) == 52
    name = construct_job_name(long_prefix, "34234234234324")
    assert len(name) == 63
    assert name.startswith("a-very-long-prefix-that-")
    assert "-34234234234324-" in name
