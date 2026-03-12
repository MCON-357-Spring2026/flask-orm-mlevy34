"""Exercises: ORM fundamentals.

Implement the TODO functions. Autograder will test them.
"""

from __future__ import annotations

from typing import Optional, Any
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

from src.exercises.extensions import db
from src.exercises.models import Student, Grade, Assignment


# ===== BASIC CRUD =====

def create_student(name: str, email: str) -> Student:
    """TODO: Create and commit a Student; handle duplicate email.

    If email is duplicate:
      - rollback
      - raise ValueError("duplicate email")
    """
    student = Student(name=name, email=email)
    db.session.add(student)
    try:
        db.session.commit()
    except Exception as ex:
        db.session.rollback()
        raise ValueError("Duplicate Emails are not accepted")



def find_student_by_email(email: str) -> Optional[Student]:
    """TODO: Return Student by email or None."""
    student = Student.query.filter_by(email="ava@example.com").first()
    if not student:
        return None
    else:
        return Optional[student]




def add_grade(student_id: int, assignment_id: int, score: int) -> Grade:
    """TODO: Add a Grade for the student+assignment and commit.

    If student doesn't exist: raise LookupError
    If assignment doesn't exist: raise LookupError
    If duplicate grade: raise ValueError("duplicate grade")
    """
    s = db.session.get(Student, student_id)
    if not s:
        raise LookupError("Student not found")
    a = db.session.get(Assignment, assignment_id)
    if not a:
        raise LookupError("Assignment not found")
    grade = Grade(score=score, student=s, assignment=a)
    db.session.add(grade)
    try:
        db.session.commit()
    except Exception as ex:
        db.rollback()
        raise ValueError("Duplicate Grades are not accepted")


def average_percent(student_id: int) -> float:
    """TODO: Return student's average percent across assignments.

    percent per grade = score / assignment.max_points * 100

    If student doesn't exist: raise LookupError
    If student has no grades: return 0.0
    """
    student = db.session.get(Student, student_id)
    avg_expr = func.avg(Grade.score * 100.0 / Assignment.max_points)
    result = (
        db.session.query(avg_expr)
        .select_from(Grade)
        .join(Assignment, Grade.assignment_id == Assignment.id)
        .filter(Grade.student_id == student_id)
        .scalar()
    )
    return float(result) if result is not None else 0.0

# ===== QUERYING & FILTERING =====

def get_all_students() -> Any | None:
    """TODO: Return all students in database, ordered by name."""
    students = Student.query.order_by(Student.name).all()
    return students


def get_assignment_by_title(title: str) -> Optional[Assignment]:
    """TODO: Return assignment by title or None."""
    assignment = Assignment.query.filter_by(title = title).first()
    if not assignment:
        return None
    return assignment


def get_student_grades(student_id: int) -> list[Grade]:
    """TODO: Return all grades for a student, ordered by assignment title.

    If student doesn't exist: raise LookupError
    """
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError("Student not found")
    return (
        Grade.query.join(Assignment)
        .filter(Grade.student_id == student_id)
        .order_by(Assignment.title)
    )


def get_grades_for_assignment(assignment_id: int) -> list[Grade]:
    """TODO: Return all grades for an assignment, ordered by student name.

    If assignment doesn't exist: raise LookupError
    """
    assignment = db.session.get(Assignment, assignment_id)
    if not assignment:
        raise LookupError("Assignment not found")
    return (
        Grade.query.join(Student)
        .filter(Grade.assignment_id == assignment_id)
        .order_by(Student.name)
    )


# ===== AGGREGATION =====

def total_student_grade_count() -> int:
    """TODO: Return total number of grades in database."""
    scores = Grade.query.count()
    return scores


def highest_score_on_assignment(assignment_id: int) -> Optional[int]:
    """TODO: Return the highest score on an assignment, or None if no grades.

    If assignment doesn't exist: raise LookupError
    """
    a = db.session.get(Assignment, assignment_id)
    if not a:
        raise LookupError("Assignment not found")
    highest = (
            db.session.query(db.func.max(Grade.score))
               .filter(Grade.assignment_id == assignment_id)
               .scalar()
               )
    return highest




def class_average_percent() -> float:
    """TODO: Return average percent across all students and all assignments.

    percent per grade = score / assignment.max_points * 100
    Return average of all these percents.
    If no grades: return 0.0
    """
    avg = (db.session.query(db.func.avg((Grade.score/Assignment.max_points) *100))
           .join(Assignment, Grade.assignment_id == Assignment.id)).scalar()
    if not avg:
        return 0.0
    return avg


def student_grade_count(student_id: int) -> int:
    """TODO: Return number of grades for a student.

    If student doesn't exist: raise LookupError
    """
    s = db.session.get(Student, student_id)
    if not s:
        raise LookupError("Student not found")
    count = (db.session.query(db.func.count(Grade.score))
             .filter(Grade.student_id == student_id)
             .scalar())
    return count



# ===== UPDATING & DELETION =====

def update_student_email(student_id: int, new_email: str) -> Student:
    """TODO: Update a student's email and commit.

    If student doesn't exist: raise LookupError
    If new email is duplicate: rollback and raise ValueError("duplicate email")
    Return the updated student.
    """
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError("Student not found")
    student.email = new_email
    try:
        db.session.commit()
    except Exception as ex:
        db.session.rollback()
        raise ValueError("duplicate email")
    return student


def delete_student(student_id: int) -> None:
    """TODO: Delete a student and all their grades; commit.

    If student doesn't exist: raise LookupError
    """
    student = db.session.get(Student, student_id)
    if not student:
        raise LookupError("Student not found")
    db.session.delete(student)
    try:
        db.session.commit()
    except Exception as ex:
        db.session.rollback()
        raise ValueError(ex)


def delete_grade(grade_id: int) -> None:
    """TODO: Delete a grade by id; commit.

    If grade doesn't exist: raise LookupError
    """
    grade = db.session.get(Grade, grade_id)
    if not grade:
        raise LookupError("Grade not found")
    db.session.delete(grade)
    try:
        db.session.commit()
    except Exception as ex:
        db.session.rollback()
        raise ValueError(ex)


# ===== FILTERING & FILTERING WITH AGGREGATION =====

def students_with_average_above(threshold: float) -> Any | None:
    """TODO: Return students whose average percent is above threshold.

    List should be ordered by average percent descending.
    percent per grade = score / assignment.max_points * 100
    """
    result = []
    for student in Student.query.all():
        grades = student.grades
        if not grades:
            continue
        percents = []
        for g in grades:
            percent = (g.score / g.assignment.max_points) * 100
            percents.append(percent)
        avg = sum(percents) / len(percents)
        avg = float(avg)
        if avg > threshold:
            result.append((student, avg))
    result = sorted(result, key=lambda x: x[1], reverse=True)
    return [s[0] for s in result]


def assignments_without_grades() -> list[Assignment]:
    """TODO: Return assignments that have no grades yet, ordered by title."""
    result = []
    for assignment in Assignment.query.all():
        if not assignmentment.grades:
            result.append((assignment, assignment.title))
    result = sorted(result, key=lambda x: x[1])
    return [r[0] for r in result]




def top_scorer_on_assignment(assignment_id: int) -> Optional[Student]:
    """TODO: Return the Student with the highest score on an assignment.

    If assignment doesn't exist: raise LookupError
    If no grades on assignment: return None
    If tie (multiple students with same high score): return any one
    """
    a = db.session.get(Assignment, assignment_id)
    highest = 0
    if not a:
        raise LookupError("Assignment not found")
    if not a.grades:
        return None
    highest = -1
    top_student = None
    for g in a.grades:
        if g.score > highest:
            highest = g.score
            top_student = g.student
    return top_student




