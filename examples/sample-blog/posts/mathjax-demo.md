---
title: "MathJax Rendering Demo"
date: "2026-03-07"
author: "blogr"
description: "Demonstrates LaTeX math rendering with MathJax"
tags: ["math", "mathjax", "demo"]
status: "published"
slug: "mathjax-demo"
featured: true
---

# MathJax Rendering Demo

This post demonstrates how blogr renders LaTeX math expressions using MathJax.
You can use inline math with single dollar signs and display math with double
dollar signs.

## Inline Math

Einstein's famous equation $E = mc^{2}$ shows the equivalence of mass and
energy. The quadratic formula $x = \frac{-b \pm \sqrt{b^2 - 4ac}}{2a}$ gives
the roots of any quadratic equation $ax^2 + bx + c = 0$.

## Display Math

The Gaussian integral:

$$\int_{-\infty}^{\infty} e^{-x^2} \, dx = \sqrt{\pi}$$

Euler's identity:

$$e^{i\pi} + 1 = 0$$

A summation formula:

$$\sum_{n=1}^{\infty} \frac{1}{n^2} = \frac{\pi^2}{6}$$

## Matrices

A 2x2 matrix and its determinant:

$$A = \begin{pmatrix} a & b \\ c & d \end{pmatrix}, \quad \det(A) = ad - bc$$

## Calculus

The fundamental theorem of calculus:

$$\int_a^b f'(x) \, dx = f(b) - f(a)$$

Taylor series expansion of $e^x$:

$$e^x = \sum_{n=0}^{\infty} \frac{x^n}{n!} = 1 + x + \frac{x^2}{2!} + \frac{x^3}{3!} + \cdots$$

## Mixing Math and Code

You can combine math with code blocks. Here is a Python implementation of the
quadratic formula $x = \frac{-b \pm \sqrt{b^2 - 4ac}}{2a}$:

```python
import math

def quadratic(a, b, c):
    discriminant = b**2 - 4*a*c
    if discriminant < 0:
        raise ValueError("No real roots")
    x1 = (-b + math.sqrt(discriminant)) / (2*a)
    x2 = (-b - math.sqrt(discriminant)) / (2*a)
    return x1, x2
```
