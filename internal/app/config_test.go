package app

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// TestConfigPasswordMasking 锁定安全契约：任何 fmt / json 格式化路径都不得泄漏明文密码。
// 屏蔽实现依赖 type plain Config 别名切断方法递归，改动后若递归失效会栈溢出，测试直接红。
func TestConfigPasswordMasking(t *testing.T) {
	c := Config{Addr: "127.0.0.1:6379", Password: "crs-xxxx:sup3rsecret"}

	cases := []struct {
		name string
		got  string
	}{
		{"String()", c.String()},
		{"%v", fmt.Sprintf("%v", c)},
		{"%+v", fmt.Sprintf("%+v", c)},
		{"%#v", fmt.Sprintf("%#v", c)},
	}
	for _, tc := range cases {
		if strings.Contains(tc.got, "sup3rsecret") {
			t.Errorf("%s 泄漏明文密码: %s", tc.name, tc.got)
		}
		if !strings.Contains(tc.got, "***") {
			t.Errorf("%s 未输出屏蔽占位符 ***: %s", tc.name, tc.got)
		}
	}

	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	if strings.Contains(string(b), "sup3rsecret") {
		t.Errorf("MarshalJSON 泄漏明文密码: %s", b)
	}
	if !strings.Contains(string(b), `"Password":"***"`) {
		t.Errorf("MarshalJSON 未输出屏蔽占位符: %s", b)
	}
}

func TestConfigPasswordMaskingEmpty(t *testing.T) {
	c := Config{Addr: "127.0.0.1:6379"}
	if strings.Contains(c.String(), "***") {
		t.Errorf("空密码不应显示 ***（会误导为已设置密码）: %s", c.String())
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("json.Marshal 失败: %v", err)
	}
	if strings.Contains(string(b), "***") {
		t.Errorf("空密码 JSON 不应显示 ***: %s", b)
	}
}
