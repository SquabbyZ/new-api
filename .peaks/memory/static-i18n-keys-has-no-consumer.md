---
name: static-i18n-keys-has-no-consumer
description: "本项目 STATIC_I18N_KEYS 只有定义、零消费方；因此判断一个 i18n 键要不要登记，看的是它「以字面量还是以数据形式」传给 t()，而不是「新增键就一律登记」。"
metadata:
  type: convention
---

`web/src/i18n/static-keys.ts` 导出的 `STATIC_I18N_KEYS`（定义在该文件第 21 行）在**全仓库没有任何消费方**。独立核验两次（编排器 grep + QA 复核）结论一致；其余命中只有三处非消费方引用：`.agents/skills/i18n-translate/SKILL.md` 的检索提示、`web/AGENTS.md:71` 的约定文字、以及一个测试文件注释。`web/scripts/sync-i18n.mjs` 只在**locale 文件之间**互相对齐，**不扫描源码**。

`web/AGENTS.md:71` 的原文措辞本身就带了条件与替代分支：

> 同步在 `src/i18n/static-keys.ts` 中登记对应 key（**若项目用其做提取**），**或**确保文案以 `t('...')` 字面量形式出现以便扫描

既然没有消费方，「若项目用其做提取」这个条件**不成立**。所以判据是**这个键怎么传给 `t()`**：

| 键的用法 | 需要登记吗 | 例 |
|---|---|---|
| **字面量** `t('Log database')` | **不需要** —— 可被扫描覆盖 | `complete-step.tsx` 新增的 `'Log database'` |
| **作为数据**传入 `t()`（常量表里的 `labelKey` / `descriptionKey`） | **需要** —— 正则扫不到 | `DATABASE_META` 的 4 条 descriptionKey |

真正决定用户看到什么的是**7 个 locale 文件里有没有这个键**（`en`/`zh`/`zh-TW`/`fr`/`ru`/`ja`/`vi`）：缺失时 i18next 回落显示英文原文字面量，界面不会崩，但那一门语言下会露英文。

**How to apply:** 新增 i18n 键时，先判断它是字面量还是数据，再决定要不要碰 `static-keys.ts` —— 不要因为「新增了键」就一律登记，也不要因为「没登记」就认定会漏翻译。核对的重心永远放在 7 个 locale 上，且非 en 的值必须**确已翻译**（非空且不等于英文原文）。若哪天真出现了 `STATIC_I18N_KEYS` 的消费方，这条约定需要重估 —— 届时应重新验证而不是沿用。
