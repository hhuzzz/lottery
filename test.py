import re

# 原始字符串
text = "%%【玩法1(非让球胜)，购买金额：50元】，【玩法2(非让球平)，购买金额：50元】，【玩法21(1:1)，购买金额：50元】，【玩法38(进球数：0)，购买金额：50元】%%"

# 使用正则表达式提取括号中的内容和购买金额
pattern = r"【.*?\((.*?)\)，购买金额：(.*?)】"
matches = re.findall(pattern, text)

# 将提取出来的内容构造为所需的格式
result = [f"{match[0]}，购买金额：{match[1]}" for match in matches]

# 输出结果
print(result)
