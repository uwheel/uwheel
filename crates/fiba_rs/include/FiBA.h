#pragma once
#include <cassert>
#include <iostream>
#include <cmath>
#include <limits>
#include <memory>

#include "AggregationFunctions.hpp"
using namespace std;

#ifdef COLLECT_STATS
#define IF_COLLECT_STATS(x) x
#else
#define IF_COLLECT_STATS(x) ;
#endif

IF_COLLECT_STATS(static long statsCombineCount = 0);
IF_COLLECT_STATS(static long statsTotalRepairAggCount = 0);
IF_COLLECT_STATS(static long statsTotalSpineRepairCount = 0);
IF_COLLECT_STATS(static long statsTotalNumNodesRepaired = 0);
IF_COLLECT_STATS(static long statsSumRootDegree = 0);

enum Kind { classic, knuckle, finger };

template<typename _timeT, int minArity, Kind kind, typename binOpFunc,
    bool early_stopping=false>
class Aggregate {
public:
  typedef typename binOpFunc::In inT;
  typedef typename binOpFunc::Partial aggT;
  typedef typename binOpFunc::Out outT;
  typedef _timeT timeT;

private:
  static int const maxArity = 2 * minArity;

  class Node {
    typedef Node* NodeP;
    NodeP _parent;
    aggT _agg;
    int _arity;
    bool _leftSpine, _rightSpine;
    timeT _times[maxArity];
    aggT _values[maxArity];
    NodeP* _children;

    aggT recalcAgg(binOpFunc const &op) const {
      if (kind==finger) {
        if (isRoot() || (_leftSpine && _rightSpine))
          return recalcAggInner(op);
        if (_leftSpine)
          return recalcAggLeft(op);
        if (_rightSpine)
          return recalcAggRight(op);
      }
      assert(hasAggUp());
      return recalcAggUp(op);
    }

    aggT recalcAggInner(binOpFunc const &op) const {
      aggT result = binOpFunc::identity;
      if (isLeaf()) {
        for (int i=0, n=_arity-1; i<n; i++) {
          result = op.combine(result, getValue(i));
          IF_COLLECT_STATS(++statsCombineCount);
        }
      } else {
        if (_arity > 1) {
          result = op.combine(result, getValue(0));
          IF_COLLECT_STATS(++statsCombineCount);
        }
        for (int i=1, n=_arity-1; i<n; i++) {
          result = op.combine(result, getChild(i)->getAgg());
          IF_COLLECT_STATS(++statsCombineCount);
          result = op.combine(result, getValue(i));
          IF_COLLECT_STATS(++statsCombineCount);
        }
      }
      return result;
    }

    aggT recalcAggLeft(binOpFunc const &op) const {
      aggT result = recalcAggInner(op);
      if (!isLeaf()){
        result = op.combine(result, getChild(_arity-1)->getAgg());
        IF_COLLECT_STATS(++statsCombineCount);
      }
      if (!isRoot() && !_parent->isRoot()) {
        result = op.combine(result, _parent->getAgg());
        IF_COLLECT_STATS(++statsCombineCount);
      }
      return result;
    }

    aggT recalcAggRight(binOpFunc const &op) const {
      aggT result = recalcAggInner(op);
      if (!isLeaf()) {
        result = op.combine(getChild(0)->getAgg(), result);
        IF_COLLECT_STATS(++statsCombineCount);
      }
      if (!isRoot() && !_parent->isRoot()) {
        result = op.combine(_parent->getAgg(), result);
        IF_COLLECT_STATS(++statsCombineCount);
      }
      return result;
    }

    aggT recalcAggUp(binOpFunc const &op) const {
      if (isLeaf())
        return recalcAggInner(op);
      if (_arity == 1)
        return getChild(0)->getAgg();
      aggT leftAgg = getChild(0)->getAgg();
      aggT innerAgg = recalcAggInner(op);
      aggT rightAgg = getChild(_arity - 1)->getAgg();
      IF_COLLECT_STATS(++statsCombineCount);
      IF_COLLECT_STATS(++statsCombineCount);
      return op.combine(leftAgg, op.combine(innerAgg, rightAgg));
    }

    bool recalcLeftSpine() const {
      if (isRoot())
        return false;
      if (!(_parent->isRoot() || _parent->leftSpine()))
        return false;
      return this == _parent->getChild(0);
    }

    bool recalcRightSpine() const {
      if (isRoot())
        return false;
      if (!(_parent->isRoot() || _parent->rightSpine()))
        return false;
      return this == _parent->getChild(_parent->arity()-1);
    }

    void setChild(int i, NodeP node) {
      assert(0 <= i && i < _arity);
      _children[i] = node;
    }

  public:
    Node(bool isLeaf)
        : _parent(NULL),
          _agg(binOpFunc::identity),
          _arity(1),
          _leftSpine(false), _rightSpine(false),
          _children(isLeaf ? NULL : new NodeP[maxArity + 1])
    { }

    ~Node() { delete[] _children; }

    int arity() const { return _arity; }

    void becomeRoot(binOpFunc const &op) {
      assert(!isRoot() && _parent->isRoot() && _parent->arity()==1);
      _parent = NULL;
      _leftSpine = false;
      _rightSpine = false;
      if (kind==finger)
        localRepairAgg(op);
    }

    int childIndex() const {
      assert(!isRoot());
      for (int i=0, n=_parent->arity(); i<n; i++)
        if (this == _parent->getChild(i))
          return i;
      assert(false);
      return -1;
    }

    aggT getAgg() const { return _agg; }

    NodeP getChild(int i) const {
      assert(!isLeaf() && 0 <= i && i < _arity);
      return _children[i];
    }

    timeT getTime(int i) const {
      assert(0 <= i && i < _arity - 1);
      return _times[i];
    }

    aggT getValue(int i) const {
      assert(0 <= i && i < _arity - 1);
      return _values[i];
    }

    bool hasAggUp() const {
      if (kind==finger)
        return !(isRoot() || _leftSpine || _rightSpine);
      else
        return true;
    }

    int height() const {
      int result = 0;
      Node const* descendent = this;
      while (!descendent->isLeaf()) {
        descendent = descendent->getChild(0);
        result++;
      }
      return result;
    }

    bool isDescendent(Node* node) const {
      Node const* ancestor = this;
      while (ancestor != NULL) {
        if (ancestor == node)
          return true;
        ancestor = ancestor->parent();
      }
      return false;
    }

    bool isLeaf() const { return NULL == _children; }

    bool isRoot() const { return NULL == _parent; }

    bool leftSpine() const { return _leftSpine; }

    bool localCheckInvariant(binOpFunc const &op, bool strong,
                             const char* f, int l) const {
      bool ok = true;
      ok &= (0 <= _arity);
      if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
      if (strong) {
        ok &= (isRoot() || minArity <= _arity) && (_arity <= maxArity);
        if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
      } else {
        ok &= (isRoot() || minArity - 1 <= _arity) && (_arity <= maxArity + 1);
        if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
      }
      if (isLeaf()) {
        for (int i=0, n=_arity-2; i<n; i++) {
          ok &= (getTime(i) < getTime(i+1));
          if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
        }
      } else {
        for (int i=0, n=_arity-1; i<n; i++) {
          Node* leftNode = getChild(i);
          if (leftNode->arity() >= 2) {
            timeT leftTime = leftNode->getTime(leftNode->arity() - 2);
            ok &= (leftTime < getTime(i));
            if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
          }
          Node* rightNode = getChild(i + 1);
          if (rightNode->arity() >= 2) {
            timeT rightTime = rightNode->getTime(0);
            ok &= getTime(i) < rightTime;
            if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
          }
        }
        for (int i=0, n=_arity; i<n; i++) {
          ok &= (getChild(i)->parent() == this);
          if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
        }
      }
      ok &= (_leftSpine == recalcLeftSpine());
      if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
      ok &= (_rightSpine == recalcRightSpine());
      if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
      if (strong) {
        ok &= (_agg == recalcAgg(op));
        if(!ok){cerr<<f<<":"<<l<<": FAILED"<<endl<<*this; throw 0;}
      }
      return ok;
    }

    void localEvict(binOpFunc const &op, int index) {
      assert(!isLeaf() && 1 < _arity && index < _arity - 1);
      for (int i=index, n=_arity-2; i<n; i++) {
        setEntry(i, getTime(i + 1), getValue(i + 1));
        setChild(i + 1, getChild(i + 2));
      }
      if (index == _arity-2 && (isRoot() || _rightSpine))
        getChild(_arity-2)->_rightSpine = true;
      _arity--;
      localRepairAggIfUp(op);
    }

    void localEvictEntry(binOpFunc const &op, timeT time) {
      int index;
      bool found = localSearch(time, index);
      assert(found);
      for (int i=index, n=_arity-2; i<n; i++)
        setEntry(i, getTime(i + 1), getValue(i + 1));
      _arity--;
      localRepairAggIfUp(op);
    }

    void localInsert(binOpFunc const &op, timeT time, aggT value, NodeP node) {
      assert(!isLeaf() && _arity <= maxArity);
      int i = _arity - 1;
      if (i==0 || time>getTime(i-1)) {
        pushBack(op, time, value, node);
      } else {
        _arity++;
        while (i>0 && time<getTime(i-1)) {
          setEntry(i, getTime(i-1), getValue(i-1));
          setChild(i+1, getChild(i));
          i--;
        }
        setEntry(i, time, value);
        setChild(i + 1, node);
        node->_parent = this;
        localRepairAggIfUp(op);
      }
    }

    bool localInsertEntry(binOpFunc const &op, timeT time, aggT value) {
      int index;
      bool found = localSearch(time, index);
      assert((found || isLeaf()) && _arity <= maxArity);
      if (found) {
        setEntry(index, time, op.combine(getValue(index), value));
        localRepairAggIfUp(op);
      } else {
        if (_arity == 0 || index == _arity - 1) {
          pushBackEntry(op, time, value);
        } else {
          _arity++;
          for (int i=_arity-2; i>index; i--)
            setEntry(i, getTime(i - 1), getValue(i - 1));
          setEntry(index, time, value);
          localRepairAggIfUp(op);
        }
      }
      return !found;
    }

    void localRepairAggIfUp(binOpFunc const &op) {
      if (hasAggUp())
        localRepairAgg(op);
    }

    bool localRepairAgg(binOpFunc const &op) {
      IF_COLLECT_STATS(statsTotalNumNodesRepaired++);
      aggT const newAgg = recalcAgg(op);
      if (_agg != newAgg) { //careful: '!=' and '==' asymmetric for geomean
        _agg = newAgg;
        return true;
      }
      return false;
    }

    bool localSearch(timeT const& time, int& index) {
      int n;
      for (index=0, n=_arity-1; index<n; index++) {
        if (time < getTime(index))
          return false;
        else if (time == getTime(index))
          return true;
      }
      assert(index == _arity - 1);
      return false;
    }

    NodeP parent() const { return _parent; }

    void popBack(binOpFunc const &op, int howMany) {
      assert(howMany < _arity);
      _arity -= howMany;
      if (!isLeaf() && (isRoot() || _rightSpine))
        getChild(_arity - 1)->_rightSpine = true;
      localRepairAggIfUp(op);
    }

    void popFront(binOpFunc const &op, int howMany) {
      assert(howMany < _arity);
      for (int i=0, n=_arity-1-howMany; i<n; i++)
        setEntry(i, getTime(i + howMany), getValue(i + howMany));
      if (!isLeaf())
        for (int i=0, n=_arity-howMany; i<n; i++)
          setChild(i, getChild(i + howMany));
      _arity -= howMany;
      if (!isLeaf() && (isRoot() || _leftSpine))
        getChild(0)->_leftSpine = true;
      localRepairAggIfUp(op);
    }

    ostream& print(ostream& os, int const indent) const {
        (void)indent;

      /*
       * Note: commented out so Scott can compile and test
       * locally.
       *
      for (int c=0; c<indent; c++) os << "  ";
      if (isLeaf()) {
        os << "(";
        for (int i=0, n=_arity-1; i<n; i++) {
          if (i > 0)
            os << ' ';
          os << getTime(i) << "/" << getValue(i);
        }
      } else {
        os << "(" << endl;
        for (int i=0, n=_arity-1; i<n; i++) {
          getChild(i)->print(os, indent+1);
          for (int c=0; c<indent; c++) os << "  ";
          os << " " << getTime(i) << "/" << getValue(i) << endl;
        }
        getChild(_arity-1)->print(os, indent+1);
        for (int c=0; c<indent; c++) os << "  ";
      }
      os << ")/ " << _agg;
      if (isRoot()) os << " root";
      if (isLeaf()) os << " leaf";
      if (kind==finger && _leftSpine) os << " left-spine";
      if (kind==finger && _rightSpine) os << " right-spine";
      os << endl;
      */
      return os;
    }

    void pushBack(binOpFunc const &op, timeT time, aggT value, NodeP node) {
      assert(!isLeaf() && 0 < _arity && _arity <= maxArity);
      _arity++;
      setEntry(_arity-2, time, value);
      setChild(_arity-1, node);
      if (isRoot() || _rightSpine) {
        getChild(_arity-2)->_rightSpine = false;
        node->_rightSpine = true;
      }
      if (hasAggUp()) {
        _agg = op.combine(_agg, value);
        IF_COLLECT_STATS(++statsCombineCount);
        _agg = op.combine(_agg, node->getAgg());
        IF_COLLECT_STATS(++statsCombineCount);
      }
      node->_parent = this;
    }

    void pushBackEntry(binOpFunc const &op, timeT time, aggT value) {
      assert(isLeaf() && _arity <= maxArity);
      _arity++;
      setEntry(_arity - 2, time, value);
      if (hasAggUp()) {
        _agg = op.combine(_agg, value);
        IF_COLLECT_STATS(++statsCombineCount);
      }
    }

    void pushFront(binOpFunc const& op, NodeP node, timeT time, aggT value) {
      assert(!isLeaf() && 0 < _arity && _arity <= maxArity);
      _arity++;
      for (int i=_arity-2; i>=1; i--)
        setEntry(i, getTime(i - 1), getValue(i - 1));
      for (int i=_arity-1; i>=1; i--)
        setChild(i, getChild(i - 1));
      setEntry(0, time, value);
      setChild(0, node);
      if (isRoot() || _leftSpine) {
        getChild(1)->_leftSpine = false;
        node->_leftSpine = true;
      }
      if (hasAggUp()) {
        _agg = op.combine(value, _agg);
        IF_COLLECT_STATS(++statsCombineCount);
        _agg = op.combine(node->getAgg(), _agg);
        IF_COLLECT_STATS(++statsCombineCount);
      }
      node->_parent = this;
    }

    void pushFrontEntry(binOpFunc const &op, timeT time, aggT value) {
      assert(isLeaf() && _arity <= maxArity);
      _arity++;
      for (int i=_arity-2; i>=1; i--)
        setEntry(i, getTime(i-1), getValue(i-1));
      setEntry(0, time, value);
      if (hasAggUp()) {
        _agg = op.combine(value, _agg);
        IF_COLLECT_STATS(++statsCombineCount);
      }
    }

    bool rightSpine() const { return _rightSpine; }

    void setEntry(int i, timeT time, aggT value) {
      assert(0 <= i && i < _arity - 1);
      _times[i] = time;
      _values[i] = value;
    }

    void setOnlyChild(NodeP child) {
      assert(!isLeaf() && _arity==1);
      _arity = 1;
      setChild(0, child);
      child->_parent = this;
      child->_leftSpine = isRoot() || _leftSpine;
      child->_rightSpine = isRoot() || _rightSpine;
      if (hasAggUp())
        _agg = child->getAgg();
    }

    friend inline std::ostream& operator<<(std::ostream& os, Node const& x) {
      return x.print(os, 0);
    }
  };

  binOpFunc _binOp;
  Node *_root;
  Node *_leftFinger, *_rightFinger;
  size_t _size;

  void heightDecrease() {
    if (false) cout << "-- height-decrease" << endl;
    assert(_root->arity() == 1);
    Node* oldRoot = _root;
    _root = oldRoot->getChild(0);
    _root->becomeRoot(_binOp);
    delete oldRoot;
    assert(checkInvariant(__FILE__, __LINE__, _root));
  }

  void heightIncrease() {
    if (false) cout << "-- height-increase" << endl;
    Node* oldRoot = _root;
    _root = new Node(false);
    _root->setOnlyChild(oldRoot);
    assert(checkInvariant(__FILE__, __LINE__, oldRoot));
  }

  Node* leastCommonAncestor(Node* node1, Node* node2) const {
    int height1 = node1->height(), height2 = node2->height();
    while (height1 < height2) {
      node1 = node1->parent();
      height1++;
    }
    while (height1 > height2) {
      node2 = node2->parent();
      height2++;
    }
    assert(height1 == height2);
    while (node1 != node2) {
      node1 = node1->parent();
      node2 = node2->parent();
    }
    assert(node1 == node2 && node1 != NULL);
    return node1;
  }

  Node* merge(Node* parent, int nodeIdx, int siblingIdx) {
    if (false) cout << "-- merge" << endl;
    int betweenIndex = (nodeIdx < siblingIdx) ? nodeIdx : siblingIdx;
    timeT betweenTime = parent->getTime(betweenIndex);
    aggT betweenValue = parent->getValue(betweenIndex);
    Node* left = parent->getChild(betweenIndex);
    Node* right = parent->getChild(betweenIndex + 1);
    if (left->isLeaf()) {
      left->pushBackEntry(_binOp, betweenTime, betweenValue);
      for (int i=0, n=right->arity()-1; i<n; i++)
        left->pushBackEntry(_binOp, right->getTime(i), right->getValue(i));
    } else {
      left->pushBack(_binOp, betweenTime, betweenValue, right->getChild(0));
      for (int i=0, n=right->arity()-1; i<n; i++)
        left->pushBack(_binOp, right->getTime(i), right->getValue(i),
                       right->getChild(i + 1));
    }
    parent->localEvict(_binOp, betweenIndex);
    if (kind!=classic && _rightFinger == right)
      _rightFinger = left;
    delete right;
    assert(checkInvariant(__FILE__, __LINE__, left));
    return left;
  }

  void move(Node* parent, int recipientIdx, int giverIdx) {
    if (false) cout << "-- move" << endl;
    Node* recipient = parent->getChild(recipientIdx);
    Node* giver = parent->getChild(giverIdx);
    int betweenIdx = (recipientIdx < giverIdx) ? recipientIdx : giverIdx;
    timeT betweenTime = parent->getTime(betweenIdx);
    aggT betweenValue = parent->getValue(betweenIdx);
    if (recipientIdx < giverIdx) {
      timeT stolenTime = giver->getTime(0);
      aggT stolenValue = giver->getValue(0);
      parent->setEntry(betweenIdx, stolenTime, stolenValue);
      if (recipient->isLeaf()) {
        recipient->pushBackEntry(_binOp, betweenTime, betweenValue);
      } else {
        Node* stolenNode = giver->getChild(0);
        recipient->pushBack(_binOp, betweenTime, betweenValue, stolenNode);
      }
      giver->popFront(_binOp, 1);
    } else {
      timeT stolenTime = giver->getTime(giver->arity() - 2);
      aggT stolenValue = giver->getValue(giver->arity() - 2);
      parent->setEntry(betweenIdx, stolenTime, stolenValue);
      if (recipient->isLeaf()) {
        recipient->pushFrontEntry(_binOp, betweenTime, betweenValue);
      } else {
        Node* stolenNode = giver->getChild(giver->arity() - 1);
        recipient->pushFront(_binOp, stolenNode, betweenTime, betweenValue);
      }
      giver->popBack(_binOp, 1);
    }
    parent->localRepairAggIfUp(_binOp);
    assert(checkInvariant(__FILE__, __LINE__, recipient, giver));
  }

  Node* pickEvictionSibling(Node* node, int& nodeIdx, int& siblingIdx) const {
    Node* parent = node->parent();
    assert(NULL != parent && 1 < parent->arity());
    nodeIdx = node->childIndex();
    if (nodeIdx == parent->arity() - 1)
      siblingIdx = nodeIdx - 1;
    else
      siblingIdx = nodeIdx + 1;
    return parent->getChild(siblingIdx);
  }

  aggT rangeQueryRec(Node const& node, timeT tFrom, timeT tTo) const {
    timeT const TMIN = std::numeric_limits<timeT>::min();
    timeT const TMAX = std::numeric_limits<timeT>::max();
    if (tFrom == TMIN && tTo == TMAX && node.hasAggUp())
      return node.getAgg();
    aggT res = binOpFunc::identity;
    if (!node.isLeaf()) {
      timeT const tNext = node.getTime(0);
      if (tFrom < tNext) {
        res = _binOp.combine(res, rangeQueryRec(*node.getChild(0),
                                                tFrom,
                                                tNext<=tTo ? TMAX : tTo));
        IF_COLLECT_STATS(++statsCombineCount);
      }
    }
    for (int i=0, n=node.arity()-1; i<n; i++) {
      timeT tCurr = node.getTime(i);
      if (tFrom <= tCurr && tCurr <= tTo) {
        res = _binOp.combine(res, node.getValue(i));
        IF_COLLECT_STATS(++statsCombineCount);
      }
      if (!node.isLeaf() && i + 1 < n) {
        timeT tNext = node.getTime(i + 1);
        if (tCurr < tTo && tFrom < tNext) {
          res = _binOp.combine(res, rangeQueryRec(*node.getChild(i + 1),
                                                  tFrom<=tCurr ? TMIN : tFrom,
                                                  tNext<=tTo ? TMAX : tTo));
          IF_COLLECT_STATS(++statsCombineCount);
        }
      }
    }
    assert(node.isLeaf() || node.arity() > 1);
    if (!node.isLeaf()) {
      timeT tCurr = node.getTime(node.arity() - 2);
      if (tCurr < tTo) {
        res = _binOp.combine(res, rangeQueryRec(*node.getChild(node.arity()-1),
                                                tFrom<=tCurr ? TMIN : tFrom,
                                                tTo));
        IF_COLLECT_STATS(++statsCombineCount);
      }
    }
    return res;
  }

  Node* rebalanceAfterEvict(Node* node, bool* hitLeft, bool* hitRight, Node* toRepair=NULL) {
    *hitLeft = node->leftSpine();
    *hitRight = node->rightSpine();
    if (node == toRepair)
      node->localRepairAggIfUp(_binOp);
    while (!node->isRoot() && node->arity() < minArity) {
      Node* parent = node->parent();
      int nodeIndex, siblingIndex;
      Node* sibling = pickEvictionSibling(node, nodeIndex, siblingIndex);
      *hitRight |= sibling->rightSpine();
      if (sibling->arity() <= minArity) {
        node = merge(parent, nodeIndex, siblingIndex);
        if (parent->isRoot() && parent->arity() == 1)
          heightDecrease();
        else
          node = parent;
      } else {
        move(parent, nodeIndex, siblingIndex);
        node = parent;
      }
      if (node == toRepair)
        node->localRepairAggIfUp(_binOp);
      *hitLeft |= node->leftSpine();
      *hitRight |= node->rightSpine();
    }
    return node;
  }

  Node* rebalanceAfterInsert(Node* node, bool* hitLeft, bool* hitRight) {
    *hitLeft = node->leftSpine();
    *hitRight = node->rightSpine();
    while (node->arity() > maxArity) {
      if (node->isRoot()) {
        heightIncrease();
        *hitLeft = true;
        *hitRight = true;
      }
      split(node);
      node = node->parent();
      *hitLeft |= node->leftSpine();
      *hitRight |= node->rightSpine();
    }
    return node;
  }

  void repairAggs(Node* top, bool hitLeft, bool hitRight) {
    // STATS: total number of calls to repairAggs
    IF_COLLECT_STATS(statsTotalRepairAggCount++);
    IF_COLLECT_STATS(statsSumRootDegree += _root->arity());
    if (kind==finger) {
      if (!top->hasAggUp()) {
        top->localRepairAgg(_binOp);
      } else {
        while (top->hasAggUp()) {
          top = top->parent();
          if (!top->localRepairAgg(_binOp) && early_stopping) return;
        }
      }
      IF_COLLECT_STATS(bool spineRepairs = false;)
      if (top->leftSpine() || (hitLeft && top->isRoot())) {
        IF_COLLECT_STATS(spineRepairs = true;)
        Node* left = top;
        while (!left->isLeaf()) {
          left = left->getChild(0);
          left->localRepairAgg(_binOp);
        }
      }
      if (top->rightSpine() || (hitRight && top->isRoot())) {
        IF_COLLECT_STATS(spineRepairs = true;)
        Node* right = top;
        while (!right->isLeaf()) {
          right = right->getChild(right->arity() - 1);
          right->localRepairAgg(_binOp);
        }
      }
      IF_COLLECT_STATS(statsTotalSpineRepairCount += (int) spineRepairs;)
    } else {
      top = top->parent();
      while (NULL != top) {
        if (!top->localRepairAgg(_binOp) && early_stopping) return;
        top = top->parent();
      }
    }
  }

  Node* searchNode(timeT time) const {
    Node* node = _root;
    if (kind!=classic && !_root->isLeaf()) {
      if (time < _root->getTime(0)) {
        node = _leftFinger;
        while (!node->isRoot() && node->getTime(node->arity() - 2) < time)
          node = node->parent();
      } else if (_root->getTime(_root->arity() - 2) < time) {
        node = _rightFinger;
        while (!node->isRoot() && time < node->getTime(0))
          node = node->parent();
      }
    }
    while (!node->isLeaf()) {
      Node* child = NULL;
      for (int i=0, n=node->arity()-1; i<n; i++) {
        if (time < node->getTime(i)) {
          child = node->getChild(i);
          break;
        }
        if (time == node->getTime(i))
          return node;
      }
      if (child == NULL)
        child = node->getChild(node->arity() - 1);
      node = child;
    }
    return node;
  }

  Node* searchOldestNode() const {
    if (kind!=classic)
      return _leftFinger;
    Node* node = _root;
    while (!node->isLeaf())
      node = node->getChild(0);
    return node;
  }

  Node* searchYoungestNode() const {
    if (kind!=classic)
      return _rightFinger;
    Node* node = _root;
    while (!node->isLeaf())
      node = node->getChild(node->arity() - 1);
    return node;
  }

  void split(Node* left) {
    if (false) cout << "-- split" << endl;
    timeT betweenTime = left->getTime(minArity);
    aggT betweenValue = left->getValue(minArity);
    Node* right = new Node(left->isLeaf());
    left->parent()->localInsert(_binOp, betweenTime, betweenValue, right);
    if (left->isLeaf()) {
      for (int i=minArity+1,n=maxArity; i<n; i++)
        right->pushBackEntry(_binOp, left->getTime(i), left->getValue(i));
    } else {
      right->setOnlyChild(left->getChild(minArity + 1));
      for (int i=minArity+1,n=maxArity; i<n; i++)
        right->pushBack(_binOp, left->getTime(i), left->getValue(i),
                        left->getChild(i+1));
    }
    left->popBack(_binOp, minArity);
    if (kind!=classic && _rightFinger == left)
      _rightFinger = right;
    left->parent()->localRepairAggIfUp(_binOp);
    assert(checkInvariant(__FILE__, __LINE__, left, right));
  }

  void destroy(Node* node) {
    if (!node->isLeaf()) {
      for (int i = 0; i < node->arity(); i++) {
        destroy(node->getChild(i));
      }
    }
    delete node;
  }

public:
  Aggregate(binOpFunc binOp)
      : _binOp(binOp),
        _root(new Node(true)),
        _leftFinger(NULL), _rightFinger(NULL),
        _size(0)
  {
    IF_COLLECT_STATS(statsCombineCount = 0);
    if (kind!=classic) {
      _leftFinger = _root;
      _rightFinger = _root;
    }
  }

  ~Aggregate() {
    IF_COLLECT_STATS(cout << "# of combine calls: " << statsCombineCount << endl);
    IF_COLLECT_STATS(cout << "# of nodes repaired: " << statsTotalNumNodesRepaired << endl);
    IF_COLLECT_STATS(cout << "# of repair calls involving a spine: " << statsTotalSpineRepairCount << endl);
    IF_COLLECT_STATS(cout << "# of repair calls: " << statsTotalRepairAggCount << endl);
    IF_COLLECT_STATS(cout << "avg root degree when repairs were made: " <<  ((double) statsSumRootDegree) / statsTotalRepairAggCount << endl);

    destroy(_root);
  }

  aggT at(timeT const& time) {
    Node* node = searchNode(time);
    for (int i=0, n=node->arity()-1; i<n; i++)
      if (time == node->getTime(i))
        return node->getValue(i);
    return binOpFunc::identity;
  }

  bool checkInvariantRec(const char* f, int l,
                         Node* node, Node* top1, Node* top2) const {
    bool strong;
    if (top1 == NULL)
      strong = true;
    else if (node->leftSpine() || node->rightSpine())
      strong = false;
    else
      strong = node->isDescendent(top1) || node->isDescendent(top2);
    bool ok = true;
    ok &= node->localCheckInvariant(_binOp, strong, f, l);
    if (!node->isLeaf())
      for (int i=0, n=node->arity(); i<n; i++)
        ok |= checkInvariantRec(f, l, node->getChild(i), top1, top2);
    return ok;
  }

  bool checkInvariant(const char* f, int l,
                      Node* top1=NULL, Node* top2=NULL) const {
    return checkInvariantRec(f, l, _root, top1, top2);
  }

  void evict() {
    Node* leaf = searchOldestNode();
    assert(leaf->isLeaf());
    if (false) cout << "- " << (kind==finger?"finger-":(kind==knuckle?"knuckle-":"finger-")) << minArity << " evict [scott: time_point can't print] " /*<< leaf->getTime(0) << '/' << leaf->getValue(0)*/ << endl;
    leaf->popFront(_binOp, 1);
    bool hitLeft=false, hitRight=false;
    Node* topChanged = rebalanceAfterEvict(leaf, &hitLeft, &hitRight);
    repairAggs(topChanged, hitLeft, hitRight);
    _size--;
    if (false) cout << *_root;
    assert(checkInvariant(__FILE__, __LINE__));
  }

  bool evict(timeT const& time) {
    Node* node = searchNode(time);
    int index;
    bool found = node->localSearch(time, index);
    if (!found)
      return false;
    if (false) cout << "- " << (kind==finger?"finger-":(kind==knuckle?"knuckle-":"finger-")) << minArity << " evict [scott: time_point can't print] " /*<< node->getTime(index) << '/' << node->getValue(index)*/ << endl;
    Node *topChanged;
    bool hitLeft, hitRight;
    if (node->isLeaf()) {
      node->localEvictEntry(_binOp, time);
      topChanged = rebalanceAfterEvict(node, &hitLeft, &hitRight);
    } else {
      Node *left = node->getChild(index), *right = node->getChild(index + 1);
      Node* leaf;
      timeT leafTime;
      aggT leafValue;
      if (right->arity() > minArity)
        leaf = oldest(right, leafTime, leafValue);
      else
        leaf = youngest(left, leafTime, leafValue);
      leaf->localEvictEntry(_binOp, leafTime);
      node->setEntry(index, leafTime, leafValue);
      topChanged = rebalanceAfterEvict(leaf, &hitLeft, &hitRight, node);
      if (topChanged->isDescendent(node)) {
        while (topChanged != node) {
          topChanged = topChanged->parent();
          hitLeft |= topChanged->leftSpine();
          hitRight |= topChanged->rightSpine();
          topChanged->localRepairAggIfUp(_binOp);
        }
        assert(topChanged == node);
      }
    }
    repairAggs(topChanged, hitLeft, hitRight);
    _size--;
    if (false) cout << *_root;
    assert(checkInvariant(__FILE__, __LINE__));
    return true;
  }

  void insert(timeT const& time, inT const& value) {
    if (false) cout << "- " << (kind==finger?"finger-":(kind==knuckle?"knuckle-":"finger-")) << minArity << " insert [scott: time_point can't print] " /*<< time << '/' << value*/ << endl;
    Node* node = searchNode(time);
    bool found = !node->localInsertEntry(_binOp, time, _binOp.lift(value));
    assert(found || node->isLeaf());
    bool hitLeft=false, hitRight=false;
    Node* topChanged = rebalanceAfterInsert(node, &hitLeft, &hitRight);
    repairAggs(topChanged, hitLeft, hitRight);
    // only increase the size if this creates a new entry in the tree
    if (!found) { _size++; }
    if (false) cout << *_root;
    assert(checkInvariant(__FILE__, __LINE__));
  }

  void insert(inT const& val) {
    if (0 == _size) {
      insert(0, val);
    } else {
      Node* leaf = searchYoungestNode();
      assert(leaf->isLeaf());
      timeT const time = 1 + leaf->getTime(leaf->arity() - 2);
      insert(time, val);
    }
  }

  timeT oldest() const {
    timeT time;
    aggT value;
    oldest(_root, time, value);
    return time;
  }

  Node* oldest(Node* node, timeT& time, aggT& value) const {
    while (!node->isLeaf())
      node = node->getChild(0);
    time = node->getTime(0);
    value = node->getValue(0);
    return node;
  }

  outT query() const {
    if (kind==finger) {
      if (_root->isLeaf()) {
        return _binOp.lower(_root->getAgg());
      } else {
        aggT leftAgg = _leftFinger->getAgg();
        aggT rootAgg = _root->getAgg();
        aggT rightAgg = _rightFinger->getAgg();
        IF_COLLECT_STATS(++statsCombineCount);
        IF_COLLECT_STATS(++statsCombineCount);
        return _binOp.lower(_binOp.combine(leftAgg,
                                           _binOp.combine(rootAgg, rightAgg)));
      }
    } else {
      return _binOp.lower(_root->getAgg());
    }
  }

  outT range(timeT timeFrom, timeT timeTo) const {
    if (kind==finger) {
      Node *nodeFrom = searchNode(timeFrom), *nodeTo = searchNode(timeTo);
      Node* top = leastCommonAncestor(nodeFrom, nodeTo);
      return _binOp.lower(rangeQueryRec(*top, timeFrom, timeTo));
    } else {
      return _binOp.lower(rangeQueryRec(*_root, timeFrom, timeTo));
    }
  }

  std::string name() const { return "FiBA"; }

  size_t size() const {
    return size_(_root);
  }

  size_t size_(Node* node) const {
    if (node == nullptr) return 0;

    size_t result = sizeof(int) + 2 * sizeof(bool) + 2 * sizeof(Node*);
    if (!node->isLeaf()) {
      result += maxArity * sizeof(timeT) + maxArity * sizeof(aggT);

      for (int i=0, n=node->arity(); i<n; i++)
        result += size_(node->getChild(i));
    }

    return result;
  }

  size_t num_nodes() const {
    return num_nodes_(_root);
  }

  size_t num_nodes_(Node* node) const {
    if (node == nullptr) return 0;
    if(node->isLeaf()) return 1;

    size_t result = 1;
    for (int i=0, n=node->arity(); i<n; i++)
      result += num_nodes_(node->getChild(i));

    return result;
  }

  size_t num_leaves() const {
    return num_leaves_(_root);
  }

  size_t num_leaves_(Node* node) const {
    if (node == nullptr) return 0;
    if (node->isLeaf()) return 1;

    size_t result = 0;
    for (int i=0, n=node->arity(); i<n; i++)
      result += num_leaves_(node->getChild(i));

    return result;
  }

  size_t data_size() const { return _size; }

  timeT youngest() const {
    timeT time;
    aggT value;
    youngest(_root, time, value);
    return time;
  }

  Node* youngest(Node* node, timeT& time, aggT& value) const {
    while (!node->isLeaf())
      node = node->getChild(node->arity() - 1);
    time = node->getTime(node->arity() - 2);
    value = node->getValue(node->arity() - 2);
    return node;
  }

  int variant() const { return minArity; }

  std::string op_func() const { return _binOp.name(); }

  void shape() const {
    std::cout << "Shape of FiBA:" << std::endl;
//    const auto min_leaf = this->searchOldestNode();
    const auto node_size = sizeof(int) + 2 * sizeof(bool) + 2 * sizeof(Node*);
    const auto data_size = maxArity * sizeof(timeT) + maxArity * sizeof(aggT);
    std::cout << "  [depth] " << _root->height() << std::endl;
    std::cout << "  [#total_nodes] " << num_nodes() << std::endl;
    std::cout << "  [#leaf_nodes] "  << num_leaves() << std::endl;
    std::cout << "  [min leaf] node size:" << node_size << " data size: " << data_size << std::endl;
  }
};

typedef Aggregate<uint64_t, 2, Kind::finger, Sum<uint64_t>> FiBA_SUM;
typedef Aggregate<uint64_t, 4, Kind::finger, Sum<uint64_t>> FiBA_SUM_4;
typedef Aggregate<uint64_t, 8, Kind::finger, Sum<uint64_t>> FiBA_SUM_8;

std::unique_ptr<FiBA_SUM> create_fiba_with_sum();
std::unique_ptr<FiBA_SUM_4> create_fiba_4_with_sum();
std::unique_ptr<FiBA_SUM_8> create_fiba_8_with_sum();

