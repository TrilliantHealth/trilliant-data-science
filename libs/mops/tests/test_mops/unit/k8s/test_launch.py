from thds.mops.k8s.launch import construct_job_name


def test_job_name_len():
    long_prefix = "a very long Prefix that has Bad characters and Stuff"
    assert len(long_prefix) == 52
    name = construct_job_name(long_prefix, "34234234234324")
    assert len(name) == 63
    assert name.startswith("a-very-long-prefix-that-")
    assert "-34234234234324-" in name


def test_job_name_without_prefix():
    job_num = (
        "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890"
    )

    assert len(job_num) == 90
    name = construct_job_name("", job_num)
    assert len(name) == 63
    assert not name.startswith("-")


def test_job_name_prefix_with_invalid_chars_doesnt_leave_leading_or_trailing_dashes():
    prefix = "^(*&@#$--P*e@T#e$R--"

    name = construct_job_name(prefix, "234234")
    assert name.startswith("p-e-t-e-r-")
    assert name[len("p-e-t-e-r-")] != "-"
